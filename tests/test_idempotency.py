# -*- coding: utf-8 -*-
"""超时后主键幂等 / 重跑不增行用例。"""
import csv
import json
import os
import tempfile
import unittest

import requests

import sync_quotes_common as mod
from sync_quotes_common import QuoteStore, sync_one


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


class FakeResp:
    def __init__(self, content):
        self.status_code = 200
        self.content = content
        self.text = content.decode("utf-8")

    def raise_for_status(self):
        pass


def make_json_resp(symbol, trade_date, close="10.5", turnover="123456.0"):
    return FakeResp(json.dumps({"code": 0, "data": {
        "symbol": symbol, "trade_date": trade_date,
        "close": close, "turnover": turnover}}).encode("utf-8"))


class TimeoutIdempotencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.key = ("600000", "20260508")

    def test_timeout_then_success_writes_single_row(self):
        # 首次 ConnectTimeout（服务端可能已处理成功），重试拿到响应；
        # 请求层只管传输，落盘侧只认 symbol+trade_date，CSV 只能有一行。
        session = FakeSession([
            requests.ConnectTimeout("timed out"),
            make_json_resp(*self.key),
        ])
        fetch = mod.build_quote_fetcher(
            "http://x/quotes", session=session, sleep_func=lambda _s: None)
        store = QuoteStore(self.tmp)
        action = sync_one(store, self.key, fetch, trace_url="http://x/quotes")
        self.assertEqual(action, "appended")
        self.assertEqual(len(session.calls), 2)
        rows = store.read_quotes()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "600000")
        self.assertEqual(store.state_snapshot()[
                         "600000|20260508"]["status"], "done")

    def test_second_full_run_does_not_grow_csv(self):
        # 完整跑两轮：第二轮全部命中状态/CSV，quotes.csv 行数不变
        keys = [("600000", "20260508"), ("000001", "20260508")]
        session = FakeSession([make_json_resp(s, d) for s, d in keys])
        fetch = mod.build_quote_fetcher(
            "http://x/quotes", session=session, sleep_func=lambda _s: None)

        store1 = QuoteStore(self.tmp)
        for key in keys:
            self.assertEqual(sync_one(store1, key, fetch), "appended")
        first_rows = len(store1.read_quotes())
        self.assertEqual(first_rows, 2)

        # 新实例模拟进程重启：重新加载 state/csv
        store2 = QuoteStore(self.tmp)
        call_count_before = len(session.calls)
        for key in keys:
            self.assertEqual(sync_one(store2, key, fetch), "duplicate")
        self.assertEqual(len(session.calls), call_count_before,
                         "done 主键不应再发请求")
        self.assertEqual(len(store2.read_quotes()), first_rows,
                         "第二轮 CSV 不得增行")

    def test_done_state_skips_fetch_entirely(self):
        store = QuoteStore(self.tmp)
        store.mark_processing(self.key)
        store.append_validated({"symbol": "600000", "trade_date": "20260508",
                                "close": "1", "turnover": "2"})

        def boom(_key):
            raise AssertionError("done 主键不应触发请求")

        action = sync_one(QuoteStore(self.tmp), self.key, boom)
        self.assertEqual(action, "duplicate")

    def test_write_decision_ignores_ts_sign_in_url(self):
        # 同一业务主键、不同 ts/sign 的 URL，落盘侧视为同一条
        session = FakeSession([make_json_resp(*self.key)])
        fetch = mod.build_quote_fetcher(
            "http://x/quotes?ts=111&sign=aaa",
            session=session, sleep_func=lambda _s: None)
        store = QuoteStore(self.tmp)
        sync_one(store, self.key, fetch, trace_url="http://x/quotes?ts=111&sign=aaa")
        # 手动构造同主键、URL 不同的第二次处理：CSV 主键守卫必须拦截
        action, _ = store.append_validated(
            {"symbol": "600000", "trade_date": "20260508",
             "close": "999", "turnover": "999"})
        self.assertEqual(action, "duplicate")
        rows = store.read_quotes()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["close"], "10.5")


if __name__ == "__main__":
    unittest.main()
