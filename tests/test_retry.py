# -*- coding: utf-8 -*-
"""请求层重试分类 / AES 直抛 / URL 归一化用例。"""
import json
import unittest
from unittest import mock

import requests

import sync_quotes_common as mod
from sync_quotes_common import (
    AESDecryptError,
    MAX_ATTEMPTS,
    build_quote_fetcher,
    decrypt_aes,
    normalize_url,
    request_with_retry,
)
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding


class FakeResponse:
    def __init__(self, status_code=200, content=b'{"ok":1}', text=None):
        self.status_code = status_code
        self.content = content
        self.text = text if text is not None else (
            content.decode("utf-8") if isinstance(content, bytes) else content)

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err


class FakeSession:
    """按脚本依次返回响应或抛出异常的假 session，并记录调用。"""

    def __init__(self, scripted):
        self.scripted = list(scripted)
        self.calls = []
        self.urls = []

    def request(self, method, url, timeout=None, **kwargs):
        self.calls.append((method, url, timeout))
        self.urls.append(url)
        item = self.scripted.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class RetryClassificationTest(unittest.TestCase):
    def setUp(self):
        # 冻结抖动：退避延迟精确等于基准值，方便断言 1/2/4 序列
        patcher = mock.patch.object(mod.random, "uniform",
                                    side_effect=lambda a, b: (a + b) / 2)
        self.uniform_mock = patcher.start()
        self.addCleanup(patcher.stop)

    def test_connect_timeout_retried_four_times_then_raised(self):
        # ConnectTimeout 连续 4 次：发满 4 次请求、按 1/2/4s 退避后上抛
        timeout = requests.ConnectTimeout("read timed out")
        sleeps = []
        session = FakeSession([timeout] * MAX_ATTEMPTS)
        with self.assertRaises(requests.ConnectTimeout):
            request_with_retry(
                "GET", "http://x/quotes", session=session,
                sleep_func=sleeps.append)
        self.assertEqual(len(session.calls), MAX_ATTEMPTS)
        # 基准退避 1s、2s、4s（抖动被冻结为中值）
        self.assertEqual(sleeps, [1.0, 2.0, 4.0])

    def test_5xx_retried_then_success(self):
        sleeps = []
        session = FakeSession([
            FakeResponse(status_code=503, content=b""),
            FakeResponse(status_code=200, content=b'{"code":0}'),
        ])
        resp = request_with_retry("GET", "http://x/quotes",
                                  session=session, sleep_func=sleeps.append)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(sleeps, [1.0])

    def test_5xx_four_attempts_raises(self):
        session = FakeSession([FakeResponse(status_code=500, content=b"err")]
                              * MAX_ATTEMPTS)
        sleeps = []
        with self.assertRaises(requests.HTTPError):
            request_with_retry("GET", "http://x/quotes",
                               session=session, sleep_func=sleeps.append)
        self.assertEqual(len(session.calls), MAX_ATTEMPTS)
        self.assertEqual(sleeps, [1.0, 2.0, 4.0])

    def test_empty_body_retried_then_success(self):
        session = FakeSession([
            FakeResponse(status_code=200, content=b""),
            FakeResponse(status_code=200, content=b'{"code":0}'),
        ])
        sleeps = []
        resp = request_with_retry("GET", "http://x/quotes",
                                  session=session, sleep_func=sleeps.append)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(sleeps, [1.0])

    def test_4xx_raised_immediately_without_retry(self):
        session = FakeSession([FakeResponse(status_code=401, content=b"bad token")])
        sleeps = []
        with self.assertRaises(requests.HTTPError):
            request_with_retry("GET", "http://x/quotes",
                               session=session, sleep_func=sleeps.append)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(sleeps, [])

    def test_404_not_retried(self):
        session = FakeSession([FakeResponse(status_code=404, content=b"no")])
        with self.assertRaises(requests.HTTPError):
            request_with_retry("GET", "http://x/quotes",
                               session=session, sleep_func=lambda _s: None)
        self.assertEqual(len(session.calls), 1)


class AESDirectRaiseTest(unittest.TestCase):
    def setUp(self):
        self.key = b"0123456789abcdef"
        self.iv = b"abcdef9876543210"

    def _encrypt(self, plain: bytes) -> bytes:
        padder = sym_padding.PKCS7(128).padder()
        padded = padder.update(plain) + padder.finalize()
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.iv))
        enc = cipher.encryptor()
        return enc.update(padded) + enc.finalize()

    def test_roundtrip_and_garbage_raises_domain_error(self):
        ciphertext = self._encrypt(json.dumps({"code": 0}).encode())
        self.assertIn(b"code", decrypt_aes(ciphertext, self.key, self.iv))
        with self.assertRaises(AESDecryptError):
            decrypt_aes(b"not-a-valid-ciphertext-block!!", self.key, self.iv)

    def test_aes_failure_propagates_without_retry(self):
        # 密文解密确定性失败：请求层不得重试，只发一次请求
        session = FakeSession([
            FakeResponse(status_code=200,
                         content=b"garbage-padding-bytes-1234567890"),
        ])
        fetch = build_quote_fetcher(
            "http://x/quotes", session=session,
            aes_key=self.key, aes_iv=self.iv,
            sleep_func=lambda _s: self.fail("解密失败不应触发退避 sleep"))
        with self.assertRaises(AESDecryptError):
            fetch(("600000", "20260508"))
        self.assertEqual(len(session.calls), 1)


class NormalizeUrlTest(unittest.TestCase):
    def test_strips_ts_and_sign_only_for_tracing(self):
        url = ("https://apphis.longhuvip.com/w1/api/quotes"
               "?symbol=600000&trade_date=20260508&ts=1759419993&sign=abc123")
        normalized = normalize_url(url)
        self.assertNotIn("ts=", normalized)
        self.assertNotIn("sign=", normalized)
        self.assertIn("symbol=600000", normalized)
        self.assertIn("trade_date=20260508", normalized)

    def test_normalize_keeps_other_params(self):
        url = "http://x/p?a=1&ts=9&sign=z&b=2"
        normalized = normalize_url(url)
        self.assertIn("a=1", normalized)
        self.assertIn("b=2", normalized)
        self.assertNotIn("ts=", normalized)


if __name__ == "__main__":
    unittest.main()
