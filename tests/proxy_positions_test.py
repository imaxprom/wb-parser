import unittest
from unittest.mock import patch

import config
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


if __name__ == "__main__":
    unittest.main()
