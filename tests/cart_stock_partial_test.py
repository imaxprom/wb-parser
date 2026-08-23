import unittest
from unittest.mock import Mock

from cart_stock_worker import CartStockWorker


class CartStockPartialResultTest(unittest.TestCase):
    def test_collect_delivers_missing_rows_without_failing_the_batch(self):
        worker = CartStockWorker.__new__(CartStockWorker)
        worker._request_wb_batch = Mock(return_value=[{
            "id": 100,
            "name": "Fresh card",
            "totalQuantity": 7,
            "sizes": [{
                "optionId": 1,
                "name": "0",
                "stocks": [{"wh": 11, "qty": 7}],
            }],
        }])
        worker.auth_metadata = Mock(return_value={"bearerExpiresAt": None})
        worker.last_wb_success_at = None
        worker.last_error = "old error"
        worker.auth_state = "unknown"

        result = worker.collect({
            "jobId": 1,
            "claimToken": "claim",
            "articles": ["100", "200"],
        })

        self.assertEqual(result["status"], "success")
        self.assertEqual([row["articleWB"] for row in result["products"]], ["100", "200"])
        self.assertFalse(result["products"][0]["missing"])
        self.assertTrue(result["products"][1]["missing"])
        self.assertEqual(worker.auth_state, "ok")
        self.assertIsNone(worker.last_error)


if __name__ == "__main__":
    unittest.main()
