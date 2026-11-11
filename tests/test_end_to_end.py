# -*- coding: utf-8 -*-
"""端到端编排 / 入口脚本 / 两轮流水中不增行的集成用例。"""
import csv
import json
import os
import tempfile
import unittest
from unittest import mock

import requests

import sync_quotes_common as mod
import sync_quotes_batch as batch_mod
import sync_quotes_raw as raw_mod
from sync_quotes_common import QuoteStore, run_quotes_sync, sync_one


class FakeResp:
    def __init__(self, payload, status_code=200, content=None):
        self.status_code = status_code
        if content is None:
            content = json.dumps(payload).encode("utf-8")
        self.content = content
        self.text = content.decode("utf-8") if content else ""

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, timeout=None, **kwargs):
        self.calls.append(url)
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def quote_resp(symbol, trade_date, close=None, turnover=None):
    return FakeResp({"code": 0, "data": {
        "symbol": symbol, "trade_date": trade_date,
        "close": close if close is not None else "10.5",
        "turnover": turnover if turnover is not None else "123456.0"}})


class FullFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.symbols = [f"{600000 + i:06d}" for i in range(10)]
        self.keys = [(s, "20260508") for s in self.symbols]

    def test_two_runs_second_run_appends_nothing(self):
        session = FakeSession([quote_resp(s, d) for s, d in self.keys])
        kwargs = dict(session=session, sleep_func=lambda _s: None)
        stats1 = run_quotes_sync(self.keys, "http://x/quotes", self.tmp, **kwargs)
        self.assertEqual(stats1["appended"], 10)
        self.assertEqual(stats1["failed"], 0)
        rows_after_first = len(QuoteStore(self.tmp).read_quotes())
        self.assertEqual(rows_after_first, 10)
        calls_after_first = len(session.calls)

        # 第二轮：全部命中，不允许发任何请求、CSV 不增行
        with self.assertLogs("sync_quotes", level="INFO") as logs:
            stats2 = run_quotes_sync(self.keys, "http://x/quotes", self.tmp, **kwargs)
        self.assertEqual(stats2["duplicate"], 10)
        self.assertEqual(stats2["appended"], 0)
        self.assertEqual(len(session.calls), calls_after_first)
        self.assertEqual(len(QuoteStore(self.tmp).read_quotes()), rows_after_first)
        self.assertTrue(any("已 done" in line or "跳过" in line
                            for line in logs.output),
                        f"第二轮应有命中日志，实际: {logs.output}")

    def test_transient_5xx_recovered_inside_flow(self):
        # 第一个主键先 500 再成功：整体仍应 10 appended、11 次请求
        responses = [FakeResp({}, status_code=500, content=b"busy"),
                     quote_resp(*self.keys[0])]
        responses += [quote_resp(s, d) for s, d in self.keys[1:]]
        session = FakeSession(responses)
        stats = run_quotes_sync(
            self.keys, "http://x/quotes", self.tmp,
            session=session, sleep_func=lambda _s: None)
        self.assertEqual(stats["appended"], 10)
        self.assertEqual(len(session.calls), 11)

    def test_4xx_counts_failed_and_keeps_processing(self):
        responses = [FakeResp({}, status_code=403, content=b"forbidden")]
        responses += [quote_resp(s, d) for s, d in self.keys[1:]]
        session = FakeSession(responses)
        stats = run_quotes_sync(
            self.keys, "http://x/quotes", self.tmp,
            session=session, sleep_func=lambda _s: None)
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["appended"], 9)
        # 4xx 只尝试一次
        self.assertEqual(len(session.calls), 10)
        # processing 保留，重跑时先走 CSV 回查（未命中再请求）
        snapshot = QuoteStore(self.tmp).state_snapshot()
        self.assertEqual(snapshot[f"{self.keys[0][0]}|20260508"]["status"],
                         "processing")

    def test_quarantine_visible_in_full_flow(self):
        responses = [
            quote_resp("60000", "20260508"),                       # 非法代码
            quote_resp(self.symbols[1], "20260508", close="bad"),  # 坏数值
        ]
        responses += [quote_resp(s, d)
                      for s, d in self.keys[2:]]
        session = FakeSession(responses)
        with self.assertLogs("sync_quotes", level="WARNING") as logs:
            stats = run_quotes_sync(
                self.keys, "http://x/quotes", self.tmp,
                session=session, sleep_func=lambda _s: None)
        self.assertEqual(stats["quarantined"], 2)
        self.assertEqual(stats["appended"], 8)
        store = QuoteStore(self.tmp)
        self.assertEqual(len(store.read_quotes()), 8)
        bad = store.read_bad_rows()
        self.assertEqual(len(bad), 2)
        self.assertTrue(any(r["symbol"] == "60000" for r in bad))
        self.assertTrue(any(r["close"] == "bad" for r in bad))
        self.assertTrue(any("坏行隔离" in line for line in logs.output))


class EntryModuleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_batch_expand_dedup_and_order(self):
        keys = batch_mod.expand_keys(
            ["600000", "000001", "600000"], ["20260508", "20260511"])
        self.assertEqual(keys, [
            ("600000", "20260508"), ("600000", "20260511"),
            ("000001", "20260508"), ("000001", "20260511"),
        ])

    def test_batch_symbols_file_and_main(self):
        sym_file = os.path.join(self.tmp, "symbols.txt")
        with open(sym_file, "w", encoding="utf-8") as fh:
            fh.write("# 注释行\n600000\n\n000001\n")
        captured = {}

        def fake_run(keys, base_url, out_dir, **kwargs):
            captured.update(keys=keys, base_url=base_url,
                            out_dir=out_dir, kwargs=kwargs)
            return {"total": 2, "appended": 2, "duplicate": 0,
                    "recovered": 0, "quarantined": 0, "failed": 0}

        with mock.patch.object(batch_mod, "run_quotes_sync", side_effect=fake_run):
            rc = batch_mod.main([
                "--symbols-file", sym_file,
                "--trade-dates", "20260508",
                "--base-url", "http://local/quotes",
                "--out-dir", os.path.join(self.tmp, "out"),
            ])
        self.assertEqual(rc, 0)
        self.assertEqual(captured["keys"],
                         [("600000", "20260508"), ("000001", "20260508")])

    def test_raw_input_csv_and_main(self):
        input_csv = os.path.join(self.tmp, "keys.csv")
        with open(input_csv, "w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["symbol", "trade_date"])
            writer.writerow(["600000", "20260508"])
            writer.writerow(["600000", "20260508"])  # 重复主键应去重
        loaded = raw_mod.load_keys_from_csv(input_csv)
        self.assertEqual(loaded, [("600000", "20260508")])

        with mock.patch.object(raw_mod, "run_quotes_sync",
                               return_value={"total": 1, "appended": 1,
                                             "duplicate": 0, "recovered": 0,
                                             "quarantined": 0, "failed": 0}):
            rc = raw_mod.main([
                "--input", input_csv,
                "--base-url", "http://local/quotes",
                "--out-dir", os.path.join(self.tmp, "out"),
            ])
        self.assertEqual(rc, 0)

    def test_raw_single_key_validation(self):
        rc = raw_mod.main([
            "--symbol", "60000",  # 非法代码
            "--trade-date", "20260508",
            "--out-dir", os.path.join(self.tmp, "out"),
        ])
        self.assertEqual(rc, 2)


if __name__ == "__main__":
    unittest.main()
