from __future__ import annotations
import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark not installed — skipping Spark tests")
class TestSparkProcessor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from src.spark_processor import SparkProcessor
        cls.spark = SparkProcessor._get_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    # ── helpers ─────────────────────

    def _make_tsv(self, rows: list[dict]) -> str:
        fields = [
            "hit_time_gmt", "date_time", "user_agent", "ip", "event_list",
            "geo_city", "geo_region", "geo_country", "pagename",
            "page_url", "product_list", "referrer",
        ]
        f = tempfile.NamedTemporaryFile(
            mode="w", suffix=".tsv", delete=False, encoding="utf-8"
        )
        writer = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                                extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            full = {k: "" for k in fields}
            full.update(row)
            writer.writerow(full)
        f.close()
        return f.name

    def _run(self, rows: list[dict]) -> dict:
        from src.spark_processor import SparkProcessor
        path = self._make_tsv(rows)
        try:
            return SparkProcessor(path).process()
        finally:
            os.unlink(path)

    # ── test cases (same as the cases with TestChunkedProcessor) ─────────────────────────────

    def test_basic_attribution(self):
        result = self._run([
            {"hit_time_gmt": "1000", "ip": "1.1.1.1",
             "referrer": "http://www.google.com/search?q=ipod"},
            {"hit_time_gmt": "2000", "ip": "1.1.1.1",
             "event_list": "1", "product_list": "E;ipod;1;290;"},
        ])
        self.assertAlmostEqual(result[("google.com", "ipod")], 290.0)

    def test_no_search_referrer_not_attributed(self):
        result = self._run([
            {"hit_time_gmt": "1000", "ip": "2.2.2.2",
             "referrer": "http://www.esshopzilla.com/",
             "event_list": "1", "product_list": "E;ipod;1;290;"},
        ])
        self.assertEqual(len(result), 0)

    def test_multiple_visitors_same_keyword_aggregated(self):
        result = self._run([
            {"hit_time_gmt": "1000", "ip": "3.3.3.3",
             "referrer": "http://www.google.com/search?q=zune"},
            {"hit_time_gmt": "1001", "ip": "3.3.3.3",
             "event_list": "1", "product_list": "E;zune;1;100;"},
            {"hit_time_gmt": "2000", "ip": "4.4.4.4",
             "referrer": "http://www.google.com/search?q=zune"},
            {"hit_time_gmt": "2001", "ip": "4.4.4.4",
             "event_list": "1", "product_list": "E;zune;1;150;"},
        ])
        self.assertAlmostEqual(result[("google.com", "zune")], 250.0)

    def test_last_touch_wins(self):
        result = self._run([
            {"hit_time_gmt": "1000", "ip": "5.5.5.5",
             "referrer": "http://www.google.com/search?q=headphones"},
            {"hit_time_gmt": "2000", "ip": "5.5.5.5",
             "referrer": "http://www.bing.com/search?q=headphones"},
            {"hit_time_gmt": "3000", "ip": "5.5.5.5",
             "event_list": "1", "product_list": "E;HP;1;199;"},
        ])
        self.assertIn(("bing.com", "headphones"), result)
        self.assertNotIn(("google.com", "headphones"), result)

    def test_full_sample_data(self):
        sample = PROJECT_ROOT / "data" / "data.sql"
        if not sample.exists():
            self.skipTest(f"Sample file not found: {sample}")
        from src.spark_processor import SparkProcessor
        result = SparkProcessor(str(sample)).process()
        self.assertAlmostEqual(result.get(("google.com", "ipod"), 0), 480.0)
        self.assertAlmostEqual(result.get(("bing.com",   "zune"), 0), 250.0)



if __name__ == "__main__":
    unittest.main(verbosity=2)
