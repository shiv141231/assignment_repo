from __future__ import annotations
SEARCH_ENGINE_MAP: dict[str, str] = {
    "google.com":           "google.com",
    "www.google.com":       "google.com",
    "google.co.uk":         "google.com",
    "www.google.co.uk":     "google.com",
    "bing.com":             "bing.com",
    "www.bing.com":         "bing.com",
    "msn.com":              "bing.com",
    "search.msn.com":       "bing.com",
    "yahoo.com":            "yahoo.com",
    "www.yahoo.com":        "yahoo.com",
    "search.yahoo.com":     "yahoo.com",
    "uk.search.yahoo.com":  "yahoo.com",
    "duckduckgo.com":       "duckduckgo.com",
    "www.duckduckgo.com":   "duckduckgo.com",
    "ask.com":              "ask.com",
    "www.ask.com":          "ask.com",
}

# Query-string params that carry the keyword, we can add more values just here in case if in future requirements changes
KEYWORD_PARAMS: list[str] = ["q", "p", "query", "qs", "text", "searchTerm", "keyword"]

#event codes
PURCHASE_EVENT: str = "1"

# product_list field index for revenue: Category;Name;Qty;Revenue;
PRODUCT_REVENUE_IDX: int = 3


CHUNK_SIZE: int = 10_000


OUTPUT_SUFFIX: str       = "_SearchKeywordPerformance.tab"
OUTPUT_HEADER: list[str] = ["Search Engine Domain", "Search Keyword", "Revenue"]
TSV_DELIMITER: str       = "\t"
