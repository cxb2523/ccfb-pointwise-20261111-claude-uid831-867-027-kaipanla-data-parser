# -*- coding: utf-8 -*-
"""脏数据隔离用例：非法代码 / 坏日期 / 坏数值全部进 quarantine。"""
import csv
import json
import os
import tempfile
import unittest

from sync_quotes_common import (
    BAD_ROWS_FILENAME,
    QUARANTINE_DIRNAME,
    QuarantineError,
    QuoteStore,
    validate_row,
)


class ValidateRowTest(unittest.TestCase):
    def test_valid_row_normalized(self):
        row = validate_row({"symbol": "600000", "trade_date": "20260508",
                            "close": "10.5", "turnover": 123456})
        self.assertEqual(row["symbol"], "600000")
        self.assertEqual(row["close"], 10.5)
        self.assertEqual(row["turnover"], 123456.0)

    def test_bad_symbol_rejected(self):
        for bad in ("60000", "6000001", "ABCDEF", "", "60000A"):
            with self.subTest(bad=bad):
                with self.assertRaises(QuarantineError) as ctx:
                    validate_row({"symbol": bad, "trade_date": "20260508",
                                  "close": "1", "turnover": "2"})
                self.assertIn("symbol", ctx.exception.reason)

    def test_bad_trade_date_rejected(self):
        for bad in ("2026-05-08", "2026050", "20261301", "20260230", ""):
            with self.subTest(bad=bad):
                with self.assertRaises(QuarantineError) as ctx:
                    validate_row({"symbol": "600000", "trade_date": bad,
                                  "close": "1", "turnover": "2"})
                self.assertIn("trade_date", ctx.exception.reason)

    def test_bad_float_rejected(self):
        for field in ("close", "turnover"):
            with self.subTest(field=field):
                good = {"symbol": "600000", "trade_date": "20260508",
                        "close": "1", "turnover": "2"}
                good[field] = "not-a-number"
                with self.assertRaises(QuarantineError) as ctx:
                    validate_row(good)
                self.assertIn(field, ctx.exception.reason)


class QuarantinePersistenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = QuoteStore(self.tmp)

    def test_bad_rows_go_to_quarantine_not_quotes(self):
        bad_rows = [
            {"symbol": "60000", "trade_date": "20260508",   # 代码只有 5 位
             "close": "10", "turnover": "20"},
            {"symbol": "000001", "trade_date": "2026-05-08",  # 日期带横线
             "close": "10", "turnover": "20"},
            {"symbol": "000002", "trade_date": "20260508",
             "close": "N/A", "turnover": "20"},               # close 坏数值
            {"symbol": "600001", "trade_date": "20260508",
             "close": "10", "turnover": None},                # turnover 空值
        ]
        for row in bad_rows:
            action, _ = self.store.append_validated(row)
            self.assertEqual(action, "quarantined")

        # quotes.csv 只有表头，没有任何数据行
        self.assertEqual(self.store.read_quotes(), [])

        bad_path = os.path.join(self.tmp, QUARANTINE_DIRNAME, BAD_ROWS_FILENAME)
        self.assertTrue(os.path.exists(bad_path))
        with open(bad_path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            self.assertEqual(reader.fieldnames,
                             ["symbol", "trade_date", "close", "turnover", "reason"])
            quarantined = list(reader)
        self.assertEqual(len(quarantined), 4)
        reasons = " ".join(r["reason"] for r in quarantined)
        self.assertIn("symbol", reasons)
        self.assertIn("trade_date", reasons)
        self.assertIn("close", reasons)
        self.assertIn("turnover", reasons)
        # 能看到具体的非法代码和坏数值
        symbols = {r["symbol"] for r in quarantined}
        self.assertIn("60000", symbols)
        self.assertIn("N/A", {r["close"] for r in quarantined})

    def test_good_and_bad_rows_mixed(self):
        good = {"symbol": "600000", "trade_date": "20260508",
                "close": "10.5", "turnover": "123456.0"}
        bad = {"symbol": "XXXXXX", "trade_date": "20260508",
               "close": "10", "turnover": "20"}
        self.assertEqual(self.store.append_validated(good)[0], "appended")
        self.assertEqual(self.store.append_validated(bad)[0], "quarantined")
        self.assertEqual(len(self.store.read_quotes()), 1)
        self.assertEqual(len(self.store.read_bad_rows()), 1)
        # 坏行不影响 good 行的 done 状态
        self.assertEqual(
            self.store.state_snapshot()["600000|20260508"]["status"], "done")

    def test_quarantine_appends_without_duplicate_header(self):
        for sym in ("60000", "60001"):
            self.store.append_validated(
                {"symbol": sym, "trade_date": "20260508",
                 "close": "1", "turnover": "2"})
        with open(os.path.join(self.tmp, QUARANTINE_DIRNAME,
                               BAD_ROWS_FILENAME), "rb") as fh:
            raw = fh.read()
        self.assertTrue(raw.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(raw.count(b"reason"), 1, "隔离文件只能有一个表头")

    def test_quarantined_is_terminal_state_on_rerun(self):
        # 坏数据是终态：重跑不重新请求、不重复刷隔离文件
        from sync_quotes_common import sync_one
        key = ("60000", "20260508")
        calls = []

        def fetch(_key):
            calls.append(_key)
            return {"symbol": "60000", "trade_date": "20260508",
                    "close": "1", "turnover": "2"}

        first = sync_one(self.store, key, fetch)
        self.assertEqual(first, "quarantined")
        self.assertEqual(len(calls), 1)
        second = sync_one(QuoteStore(self.tmp), key, fetch)
        self.assertEqual(second, "quarantined")
        self.assertEqual(len(calls), 1, "已隔离主键重跑不得再发请求")
        self.assertEqual(len(self.store.read_bad_rows()), 1,
                         "隔离文件不得重复追加同一坏行")
        snapshot = QuoteStore(self.tmp).state_snapshot()
        self.assertEqual(snapshot["60000|20260508"]["status"], "quarantined")

    def test_quarantine_state_anchored_to_request_key(self):
        # 响应里的 symbol 损坏（5 位），隔离状态也必须挂在请求主键下，
        # 重跑同一请求主键才能命中终态、不重复刷隔离文件
        from sync_quotes_common import sync_one
        req_key = ("600002", "20260508")
        calls = []

        def fetch(_key):
            calls.append(_key)
            return {"symbol": "60002", "trade_date": "20260508",
                    "close": "10.1", "turnover": "1000"}  # 坏 symbol

        action = sync_one(self.store, req_key, fetch)
        self.assertEqual(action, "quarantined")
        snapshot = self.store.state_snapshot()
        self.assertEqual(snapshot["600002|20260508"]["status"], "quarantined")
        self.assertNotIn("60002|20260508", snapshot)
        # 隔离文件保留响应原值，便于排查
        bad = self.store.read_bad_rows()
        self.assertEqual(bad[0]["symbol"], "60002")

        # 重跑：命中终态，不再请求、不再追加隔离行
        action2 = sync_one(QuoteStore(self.tmp), req_key, fetch)
        self.assertEqual(action2, "quarantined")
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(QuoteStore(self.tmp).read_bad_rows()), 1)


if __name__ == "__main__":
    unittest.main()
