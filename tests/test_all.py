from __future__ import annotations
import csv
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.parsers import parse_referrer, parse_revenue, _has_purchase
from src.processor import ChunkedProcessor
from src.writer import write_output


# ── helpers 

def _make_tsv(rows: list[dict]) -> str:
    """Write rows to a temp TSV file for testing."""
    fields = [
        "hit_time_gmt", "date_time", "user_agent", "ip", "event_list",
        "geo_city", "geo_region", "geo_country", "pagename",
        "page_url", "product_list", "referrer",
    ]
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False, encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=fields, delimiter="\t", extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        full = {k: "" for k in fields}
        full.update(row)
        writer.writerow(full)
    f.close()
    return f.name


# ── parse_referrer ──

class TestParseReferrer(unittest.TestCase):

    def test_google(self):
        self.assertEqual(
            parse_referrer("http://www.google.com/search?q=Ipod"),
            ("google.com", "ipod"),
        )

    def test_yahoo(self):
        self.assertEqual(
            parse_referrer("http://search.yahoo.com/search?p=cd+player"),
            ("yahoo.com", "cd player"),
        )

    def test_bing(self):
        self.assertEqual(
            parse_referrer("http://www.bing.com/search?q=Zune&form=QBLH"),
            ("bing.com", "zune"),
        )

    def test_msn_maps_to_bing(self):
        domain, _ = parse_referrer("http://search.msn.com/results.aspx?q=Xbox")
        self.assertEqual(domain, "bing.com")

    def test_internal_site(self):
        self.assertEqual(parse_referrer("http://www.esshopzilla.com/cart/"), (None, None))

    def test_engine_no_keyword(self):
        self.assertEqual(parse_referrer("http://www.google.com/"), (None, None))

    def test_empty(self):
        self.assertEqual(parse_referrer(""), (None, None))

    def test_none(self):
        self.assertEqual(parse_referrer(None), (None, None))


# ── parse_revenue ─────────────────────────────────────────────────────────────

class TestParseRevenue(unittest.TestCase):

    def test_single_product(self):
        self.assertAlmostEqual(parse_revenue("E;Zune;1;250;", "1"), 250.0)

    def test_multiple_products(self):
        self.assertAlmostEqual(parse_revenue("E;A;1;100;,E;B;1;50;", "1"), 150.0)

    def test_no_purchase_event(self):
        self.assertAlmostEqual(parse_revenue("E;Zune;1;250;", "2,12"), 0.0)

    def test_empty_product_list(self):
        self.assertAlmostEqual(parse_revenue("", "1"), 0.0)

    def test_none_inputs(self):
        self.assertAlmostEqual(parse_revenue(None, None), 0.0)

    def test_empty_revenue_field(self):
        self.assertAlmostEqual(parse_revenue("E;Ipod;1;;", "1"), 0.0)

    def test_malformed_product_skipped(self):
        self.assertAlmostEqual(parse_revenue("bad", "1"), 0.0)


# ── ChunkedProcessor ──────────────────────────────────────────────────────────

class TestChunkedProcessor(unittest.TestCase):

    def _run(self, rows):
        path = _make_tsv(rows)
        try:
            return ChunkedProcessor(path).process()
        finally:
            os.unlink(path)

    def test_basic_attribution(self):
        result = self._run([
            {"hit_time_gmt": "1000", "ip": "1.1.1.1",
             "referrer": "http://www.google.com/search?q=Ipod"},
            {"hit_time_gmt": "2000", "ip": "1.1.1.1",
             "event_list": "1", "product_list": "E;Ipod;1;290;"},
        ])
        self.assertAlmostEqual(result[("google.com", "ipod")], 290.0)

    def test_no_search_referrer_not_attributed(self):
        result = self._run([
            {"hit_time_gmt": "1000", "ip": "2.2.2.2",
             "referrer": "http://www.esshopzilla.com/",
             "event_list": "1", "product_list": "E;Ipod;1;290;"},
        ])
        self.assertEqual(len(result), 0)

    def test_multiple_visitors_same_keyword_aggregated(self):
        result = self._run([
            {"ip": "3.3.3.3", "referrer": "http://www.google.com/search?q=Zune"},
            {"ip": "3.3.3.3", "event_list": "1", "product_list": "E;Zune;1;100;"},
            {"ip": "4.4.4.4", "referrer": "http://www.google.com/search?q=Zune"},
            {"ip": "4.4.4.4", "event_list": "1", "product_list": "E;Zune;1;150;"},
        ])
        self.assertAlmostEqual(result[("google.com", "zune")], 250.0)

    def test_last_touch_wins(self):
        """Second search engine (Bing) seen before purchase should win."""
        result = self._run([
            {"ip": "5.5.5.5", "referrer": "http://www.google.com/search?q=headphones"},
            {"ip": "5.5.5.5", "referrer": "http://www.bing.com/search?q=headphones"},
            {"ip": "5.5.5.5", "event_list": "1", "product_list": "E;HP;1;199;"},
        ])
        self.assertIn(("bing.com", "headphones"), result)
        self.assertNotIn(("google.com", "headphones"), result)

    def test_processes_across_chunk_boundary(self):
        """Referrer in chunk N, purchase in chunk N+1 — must still attribute."""
        from src import config as cfg
        original = cfg.CHUNK_SIZE
        cfg.CHUNK_SIZE = 2          
        try:
            result = self._run([
                {"ip": "6.6.6.6", "referrer": "http://www.google.com/search?q=Nano"},
                {"ip": "6.6.6.6"},                                      
                {"ip": "6.6.6.6", "event_list": "1", "product_list": "E;Nano;1;99;"},
            ])
            self.assertAlmostEqual(result[("google.com", "nano")], 99.0)
        finally:
            cfg.CHUNK_SIZE = original

    def test_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            ChunkedProcessor("/nonexistent/file.tsv").process()


# ── write_output ──────────────────────────────────────────────────────────────

class TestWriteOutput(unittest.TestCase):

    SAMPLE_MAP = {
        ("bing.com",   "zune"):      250.0,
        ("google.com", "ipod"):      290.0,
        ("yahoo.com",  "cd player"): 190.0,
    }

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.input_path = os.path.join(self.tmp_dir, "data.sql")
        open(self.input_path, "w").close()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def _tab_files(self):
        return sorted(f for f in os.listdir(self.tmp_dir) if f.endswith(".tab"))

    def test_header_and_sorted_descending(self):
        out = write_output(self.SAMPLE_MAP, self.input_path)
        with open(out, encoding="utf-8") as fh:
            rows = list(csv.reader(fh, delimiter="\t"))
        self.assertEqual(rows[0], ["Search Engine Domain", "Search Keyword", "Revenue"])
        revenues = [float(r[2]) for r in rows[1:] if r and r[0] and r[1] not in ("TOTAL REVENUE", "TOTAL")]
        self.assertEqual(revenues, sorted(revenues, reverse=True))
        self.assertAlmostEqual(revenues[0], 290.0)

    def test_filename_follows_spec(self):
        out = write_output(self.SAMPLE_MAP, self.input_path)
        filename = os.path.basename(out)
        #today = datetime.now().strftime("%Y-%m-%d")
        today = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        # updated this logic to consider the use case when the same script is running multiple times a day then it should not override the file rather use the timestamp
        self.assertEqual(filename, f"{today}_SearchKeywordPerformance.tab",
                         f"Unexpected filename: {filename}")


# ── end-to-end against sample data 

class TestEndToEnd(unittest.TestCase):

    def test_sample_data(self):
        sample = PROJECT_ROOT / "data" / "data.sql"
        if not sample.exists():
            self.skipTest(f"Sample file not found: {sample}")

        result = ChunkedProcessor(str(sample)).process()
        self.assertAlmostEqual(result.get(("google.com", "ipod"), 0), 480.0)
        self.assertAlmostEqual(result.get(("bing.com",   "zune"), 0), 250.0)
        self.assertNotIn(("yahoo.com", "cd player"), result)


if __name__ == "__main__":
    unittest.main(verbosity=2)
