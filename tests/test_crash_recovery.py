# -*- coding: utf-8 -*-
"""CSV/state 崩溃窗口恢复用例。

覆盖两类崩溃点：
  1. 崩在 processing 之后、CSV 追加之前（CSV 无行）-> 重跑重新请求追加；
  2. 崩在 CSV flush 之后、state 置 done 之前（CSV 有行、state 停留 processing
     或 state 整体缺失）-> 重跑回查 CSV 命中，补记 done，不重复追加。
"""
import json
import os
import tempfile
import unittest

import sync_quotes_common as mod
from sync_quotes_common import (
    CSV_HEADER,
    QuoteStore,
    STATE_FILENAME,
    sync_one,
)


class FakeResp:
    def __init__(self, payload):
        self.status_code = 200
        self.content = json.dumps(payload).encode("utf-8")
        self.text = json.dumps(payload)

    def raise_for_status(self):
        pass


def row_resp(symbol, trade_date):
    return FakeResp({"code": 0, "data": {
        "symbol": symbol, "trade_date": trade_date,
        "close": "10.5", "turnover": "123456.0"}})


class CrashWindowRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.key = ("600000", "20260508")
        self.key_id = "600000|20260508"

    def _fetch(self, responses):
        session = type("S", (), {
            "calls": [],
            "request": lambda self, method, url, timeout=None, **kw: (
                self.calls.append(url) or responses.pop(0)),
        })()
        fetch = mod.build_quote_fetcher(
            "http://x/quotes", session=session,
            sleep_func=lambda _s: None)
        return session, fetch

    def test_processing_without_csv_row_is_refetched(self):
        # 崩溃点 1：state=processing，CSV 无行 -> 未命中 -> 重新请求并追加
        store = QuoteStore(self.tmp)
        store.mark_processing(self.key, trace_url="http://x/quotes")
        self.assertEqual(
            store.state_snapshot()[self.key_id]["status"], "processing")
        self.assertEqual(store.read_quotes(), [])

        session, fetch = self._fetch([row_resp(*self.key)])
        new_store = QuoteStore(self.tmp)
        action = sync_one(new_store, self.key, fetch)
        self.assertEqual(action, "appended")
        self.assertEqual(len(session.calls), 1, "CSV 未命中必须重新发请求")
        self.assertEqual(len(new_store.read_quotes()), 1)
        self.assertEqual(new_store.state_snapshot()[self.key_id]["status"], "done")

    def test_csv_row_exists_but_state_stuck_processing(self):
        # 崩溃点 2a：CSV 已 flush，state 停在 processing -> 回查命中，补记 done
        store = QuoteStore(self.tmp)
        store.mark_processing(self.key)
        action, _ = store.append_validated(
            {"symbol": "600000", "trade_date": "20260508",
             "close": "10.5", "turnover": "123456.0"})
        self.assertEqual(action, "appended")
        # 人为把 done 改回 processing，模拟置 done 前崩溃
        state = store.state_snapshot()
        state[self.key_id]["status"] = "processing"
        with open(store.state_path, "w", encoding="utf-8") as fh:
            json.dump(state, fh)

        def boom(_key):
            raise AssertionError("CSV 命中时不应再请求接口")

        recovered = QuoteStore(self.tmp)
        result = sync_one(recovered, self.key, boom)
        self.assertEqual(result, "recovered")
        snapshot = recovered.state_snapshot()[self.key_id]
        self.assertEqual(snapshot["status"], "done")
        self.assertEqual(snapshot.get("recovered_from"), "csv_reconcile")
        self.assertEqual(len(recovered.read_quotes()), 1, "不得重复追加 CSV")

    def test_state_file_missing_but_csv_has_row(self):
        # 崩溃点 2b：state 文件整体丢失，CSV 有行 -> 同样靠回查恢复
        store = QuoteStore(self.tmp)
        store.append_validated({"symbol": "600000", "trade_date": "20260508",
                                "close": "10.5", "turnover": "123456.0"})
        os.unlink(os.path.join(self.tmp, STATE_FILENAME))
        self.assertFalse(os.path.exists(os.path.join(self.tmp, STATE_FILENAME)))

        def boom(_key):
            raise AssertionError("无 state 但 CSV 命中时不应再请求接口")

        recovered = QuoteStore(self.tmp)
        result = sync_one(recovered, self.key, boom)
        self.assertEqual(result, "recovered")
        self.assertEqual(
            recovered.state_snapshot()[self.key_id]["status"], "done")
        self.assertEqual(len(recovered.read_quotes()), 1)

    def test_state_always_atomic_no_tmp_left_behind(self):
        store = QuoteStore(self.tmp)
        store.mark_processing(self.key)
        store.append_validated({"symbol": "600000", "trade_date": "20260508",
                                "close": "1", "turnover": "2"})
        leftovers = [f for f in os.listdir(self.tmp)
                     if f.startswith(".sync_state.") and f.endswith(".tmp")]
        self.assertEqual(leftovers, [])
        # state 文件始终是合法 JSON
        with open(store.state_path, encoding="utf-8") as fh:
            self.assertEqual(json.load(fh)[self.key_id]["status"], "done")

    def test_csv_header_written_once_with_bom(self):
        store = QuoteStore(self.tmp)
        store.append_validated({"symbol": "600000", "trade_date": "20260508",
                                "close": "1", "turnover": "2"})
        store.append_validated({"symbol": "000001", "trade_date": "20260508",
                                "close": "3", "turnover": "4"})
        with open(store.quotes_path, "rb") as fh:
            raw = fh.read()
        self.assertTrue(raw.startswith(b"\xef\xbb\xbf"), "quotes.csv 必须 utf-8-sig")
        # 全文件只有一个表头行
        self.assertEqual(raw.count(",".join(CSV_HEADER).encode()), 1)


if __name__ == "__main__":
    unittest.main()
