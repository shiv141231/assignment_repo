
from __future__ import annotations
import csv
import logging
from abc import ABC, abstractmethod
from collections import defaultdict

from src.config import CHUNK_SIZE, TSV_DELIMITER
from src.parsers import parse_referrer, parse_revenue

logger = logging.getLogger(__name__)

class BaseProcessor(ABC):

    def __init__(self, input_path: str):
        self.input_path = input_path

    def process(self) -> dict[tuple[str, str], float]:

        ...

    @abstractmethod
    def describe(self) -> str:
        ...


class ChunkedProcessor(BaseProcessor):

    def describe(self) -> str:
        return f"ChunkedProcessor | chunk_size={CHUNK_SIZE:,} rows | file={self.input_path}"

    def process(self) -> dict[tuple[str, str], float]:
        last_search: dict[str, tuple[str, str]] = {}         
        revenue_map: dict[tuple[str, str], float] = defaultdict(float) 

        total_rows = 0
        purchase_rows = 0

        for chunk in self._iter_chunks():
            for row in chunk:
                total_rows += 1
                ip           = (row.get("ip")           or "").strip()
                referrer     = (row.get("referrer")     or "").strip()
                event_list   = (row.get("event_list")   or "").strip()
                product_list = (row.get("product_list") or "").strip()

             
                domain, keyword = parse_referrer(referrer)
                if domain and keyword:
                    last_search[ip] = (domain, keyword)

         
                revenue = parse_revenue(product_list, event_list)
                if revenue > 0 and ip in last_search:
                    purchase_rows += 1
                    key = last_search[ip]
                    revenue_map[key] += revenue
                    logger.debug("$%.2f → %s / '%s'  (ip=%s)", revenue, key[0], key[1], ip)

        logger.info(
            "Processed %s rows | purchases attributed: %d | unique (engine,keyword): %d",
            f"{total_rows:,}", purchase_rows, len(revenue_map),
        )
        return dict(revenue_map)

    def _iter_chunks(self):

        with open(self.input_path, encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh, delimiter=TSV_DELIMITER)
            chunk = []
            for row in reader:
                chunk.append(row)
                if len(chunk) == CHUNK_SIZE:
                    yield chunk
                    chunk = []      
            if chunk:              
                yield chunk
