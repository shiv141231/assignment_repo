
from __future__ import annotations
from urllib.parse import urlparse, parse_qs
from src.config import SEARCH_ENGINE_MAP, KEYWORD_PARAMS, PURCHASE_EVENT, PRODUCT_REVENUE_IDX


def parse_referrer(referrer: str | None) -> tuple[str | None, str | None]:

    if not referrer:
        return None, None
    try:
        parsed = urlparse(referrer)
    except ValueError:
        return None, None

    hostname = (parsed.hostname or "").lower()
    domain = SEARCH_ENGINE_MAP.get(hostname)
    if not domain:
        return None, None

    qs = parse_qs(parsed.query)
    for param in KEYWORD_PARAMS:
        values = qs.get(param)
        if values and values[0].strip():
            return domain, values[0].strip().lower()
            #return domain, values[0].strip()
            # this is the condition which captures if the end user is searching using keyword like Ipod or ipod .. its same product so revenue is capturing both as same ..

    return None, None  


def parse_revenue(product_list: str | None, event_list: str | None) -> float:
    if not _has_purchase(event_list or "") or not product_list:
        return 0.0

    total = 0.0
    for product in product_list.split(","):
        fields = product.split(";")
        if len(fields) > PRODUCT_REVENUE_IDX:
            raw = fields[PRODUCT_REVENUE_IDX].strip()
            if raw:
                try:
                    total += float(raw)
                except ValueError:
                    pass
    return total


def _has_purchase(event_list: str) -> bool:
    return PURCHASE_EVENT in {e.strip() for e in event_list.split(",") if e.strip()}
