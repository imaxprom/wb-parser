import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import config
import parser
import proxy_positions


class ProxyPositionsTest(unittest.TestCase):
    def test_uses_working_wb_search_host_and_configured_destination(self):
        self.assertEqual(
            proxy_positions.SEARCH_URL,
            "https://search.wb.ru/exactmatch/ru/common/v18/search",
        )
        self.assertEqual(proxy_positions.DEST, int(config.WB_DEST))

    def test_ad_position_without_top_600_organic_position_is_valid(self):
        sku = 991056121
        advertised = {"id": sku, "logs": "promo", "meta": {"presetId": 10}}
        other = {"id": 1, "logs": ""}
        responses = [
            ({"products": [advertised]}, None),
            ({"products": []}, None),
            ({"products": [other]}, None),
            ({"products": []}, None),
        ]

        with (
            patch.object(proxy_positions, "_build_headers", return_value={}),
            patch.object(proxy_positions, "_search_sync", side_effect=responses),
        ):
            result = proxy_positions._fetch_keyword_sync("", "query", sku, -1257786)

        self.assertEqual(result["promo_pos"], 1)
        self.assertIsNone(result["organic_pos"])
        self.assertTrue(result["is_advertised"])
        self.assertFalse(result["error"])

    def test_geo_scan_uses_working_search_host_and_current_auth(self):
        response = SimpleNamespace(
            status_code=200,
            json=lambda: {"products": [{"id": 123}]},
        )
        region = {"name": "Москва", "short": "МСК", "dest": "-1257786"}

        with (
            patch.object(
                proxy_positions,
                "_build_headers",
                return_value={"Authorization": "Bearer current"},
            ) as build_headers,
            patch("curl_cffi.requests.get", return_value=response) as request,
        ):
            result = asyncio.run(
                parser.geo_scan("123", "query", [region], pages_depth=1)
            )

        build_headers.assert_called_once_with("__direct__")
        self.assertEqual(request.call_args.args[0], proxy_positions.SEARCH_URL)
        self.assertEqual(
            request.call_args.kwargs["headers"]["Authorization"],
            "Bearer current",
        )
        self.assertEqual(result[0]["position"], 1)


if __name__ == "__main__":
    unittest.main()
