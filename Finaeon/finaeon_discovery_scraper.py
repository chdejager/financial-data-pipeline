#!/usr/bin/env python3
"""
GFD Symbol Discovery and Time Series Data Scraper

Script to:
1. Authenticate with GFD API
2. Discover symbols using paging-aware (adaptive) discovery
3. Check what's already scraped in output directories
4. Batch scrape remaining symbols
5. Save manifests with results

Usage:
    python finaeon_discovery_scraper.py
"""

import requests
import pandas as pd
import json
import os
import time
import glob
from datetime import datetime as dt
import argparse
import sys
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

# CONFIGURATION

# API Configuration
GFD_API_URL = os.environ.get("GFD_API_URL", "https://api.finaeon.com/")
# Trial/demo credentials (as originally used in this repo).
# Note: requests are still logged with password/token redacted.
GFD_USERNAME = "tryapi@finaeon.com"
GFD_PASSWORD = "Test!123"

# Output Configuration
# Configure where scraped data and manifests are saved.
# See README.md for setup instructions.
SERIES_DIR = os.path.expanduser(
    os.environ.get("FINAEON_SERIES_DIR", "~/finaeon_data/series")
)
MANIFEST_DIR = os.path.expanduser(
    os.environ.get("FINAEON_MANIFEST_DIR", "~/finaeon_data/manifest")
)

# Scraping defaults
# If start/end are None, they are omitted from the /series request so the API can return the
# earliest/latest available data.
DEFAULT_START_DATE = None
DEFAULT_END_DATE = None
DEFAULT_PERIODICITY = "monthly"
DEFAULT_MAX_PAGES = 5
DEFAULT_PAGE_SIZE = 100
DEFAULT_DISCOVERY_STRATEGY = "adaptive"  # fixed | adaptive
DEFAULT_SEARCH_SORT = "alpha"  # pop | alpha (per API guide)
DEFAULT_MAX_PREFIX_DEPTH = 2
DEFAULT_CHECKPOINT_EVERY_LEAVES = 25

# Character set used when subdividing prefixes (adaptive discovery)
DEFAULT_PREFIX_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"


# HELPER FUNCTIONS
# Some helper functions come directly from Finaeon website and API examples


def ensure_output_dirs():
    os.makedirs(SERIES_DIR, exist_ok=True)
    os.makedirs(MANIFEST_DIR, exist_ok=True)


def write_json_to_file(file_suffix, json_contents, directory="./responses/"):
    """Write JSON content to file with timestamp prefix."""
    now = dt.now()
    json_filename = now.strftime("%Y%m%d-%H%M%S%f") + '_' + file_suffix + '.json'
    os.makedirs(directory, exist_ok=True)
    output_filepath = os.path.join(directory, json_filename)
    with open(output_filepath, 'w') as f:
        json.dump(json_contents, f)
    return output_filepath


def _redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        redacted = {}
        for k, v in value.items():
            key = str(k).lower()
            if key in {"password", "token", "access_token", "authorization", "api_key", "apikey"}:
                redacted[k] = "[REDACTED]"
            else:
                redacted[k] = _redact_sensitive(v)
        return redacted
    if isinstance(value, list):
        return [_redact_sensitive(v) for v in value]
    return value


def _strip_token_fields(payload: Any) -> Any:
    """
    Remove token fields from API responses before writing them to disk.
    """
    if not isinstance(payload, dict):
        return payload
    cleaned = dict(payload)
    cleaned.pop("token", None)
    return cleaned


def call_api(path, parameters):
    """Call the GFD API endpoint."""
    url = GFD_API_URL.rstrip("/") + "/" + str(path).lstrip("/")
    headers = {'Content-type': 'application/json'}
    print(f"calling {url}")
    print(f"request body: {_redact_sensitive(parameters)}\n")
    
    retries = 3
    backoff_seconds = 1.0

    for attempt in range(1, retries + 1):
        try:
            return requests.post(url, headers=headers, data=json.dumps(parameters), timeout=60)
        except requests.exceptions.RequestException as e:
            msg = str(e)
            if attempt < retries:
                sleep_for = backoff_seconds * (2 ** (attempt - 1))
                print(f"[network retry {attempt}/{retries}] {type(e).__name__}: {msg[:120]} (sleep {sleep_for:.1f}s)")
                time.sleep(sleep_for)
                continue

            if "Failed to resolve" in msg or "NameResolutionError" in msg:
                raise RuntimeError(
                    "Network/DNS error calling GFD API. "
                    "Your machine can't resolve the hostname. "
                    "Check internet/VPN/DNS, or override the base URL via env var GFD_API_URL."
                ) from e
            raise RuntimeError(f"Network error calling GFD API: {e}") from e


def gfd_auth(username, password):
    """Authenticate with GFD API and return token."""
    parameters = {'username': username, 'password': password}
    resp = call_api('/login', parameters=parameters)
    
    if resp.status_code != 200:
        raise ValueError(f'GFD /login failed with HTTP {resp.status_code}: {resp.text[:300]}')
    
    json_content = resp.json()
    print(f"GFD API token received at {dt.now()}\n")
    return json_content


def _should_refresh_token_error(err: Exception) -> bool:
    msg = str(err).lower()
    return (
        "access token is expired" in msg
        or "token is expired" in msg
        or "please obtain access token" in msg
        or "token cannot be shared" in msg
        or "invalid token" in msg
        or "http 401" in msg
    )


def _refresh_token() -> str:
    auth_json = gfd_auth(GFD_USERNAME, GFD_PASSWORD)
    return auth_json["token"].strip('"')


def safe_gfd_search(token: str, *, search_string: str, **kwargs) -> Tuple[Dict[str, Any], str]:
    """
    Wrapper around `gfd_search` that refreshes the token once if it expires.

    Returns:
        (search_data, token)
    """
    try:
        return gfd_search(token, search_string=search_string, **kwargs), token
    except ValueError as e:
        if not _should_refresh_token_error(e):
            raise
        token = _refresh_token()
        return gfd_search(token, search_string=search_string, **kwargs), token


def safe_gfd_series(token: str, **kwargs) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Wrapper around `gfd_series` that refreshes the token once if it expires.

    Returns:
        (series_data_or_none, token)
    """
    try:
        return gfd_series(token, **kwargs), token
    except ValueError as e:
        if not _should_refresh_token_error(e):
            raise
        token = _refresh_token()
        return gfd_series(token, **kwargs), token


def gfd_search(token, search_string, **kwargs):
    """Search for symbols in GFD API."""
    page = kwargs.get('page', None)
    page_size = kwargs.get('pageSize', None)
    search_type = kwargs.get('searchType', None)
    base_filter = kwargs.get('baseFilter', None)
    sort = kwargs.get('sort', None)
    
    parameters = {
        'token': token,
        'page': page,
        'pageSize': page_size,
        'searchString': search_string,
        'searchType': search_type,
        'baseFilter': base_filter,
        'sort': sort
    }
    parameters = {key: val for key, val in parameters.items() if val is not None}
    
    r = call_api('/search', parameters=parameters)
    if r.status_code != 200:
        raise ValueError(f"GFD /search failed with HTTP {r.status_code}: {r.text[:300]}")
    search_data = r.json()
    return search_data


def gfd_series(token, **kwargs):
    """Fetch time series data from GFD API."""
    series_id = kwargs.get('seriesId', None)
    series_name = kwargs.get('seriesName', None)
    split_adjusted = kwargs.get('splitAdjusted', None)
    start_date = kwargs.get('startDate', None)
    end_date = kwargs.get('endDate', None)
    periodicity = kwargs.get('periodicity', None)
    close_only = kwargs.get('closeOnly', None)
    currency = kwargs.get('currency', None)
    inflation_adjusted = kwargs.get('inflationAdjusted', None)
    annual_flow = kwargs.get('annualFlow', None)
    total_return = kwargs.get('totalReturn', None)
    corporate_actions = kwargs.get('corporateActions', None)
    metadata = kwargs.get('metadata', None)
    inc_fields = kwargs.get('incFields', None)
    include_average = kwargs.get('includeAverage', None)
    period_percent_change = kwargs.get('periodPercentChange', None)
    
    parameters = {
        'token': token,
        'seriesId': series_id,
        'seriesName': series_name,
        'splitAdjusted': split_adjusted,
        'startDate': start_date,
        'endDate': end_date,
        'periodicity': periodicity,
        'closeOnly': close_only,
        'currency': currency,
        'inflationAdjusted': inflation_adjusted,
        'annualFlow': annual_flow,
        'totalReturn': total_return,
        'corporateActions': corporate_actions,
        'metadata': metadata,
        'incFields': inc_fields,
        'includeAverage': include_average,
        'periodPercentChange': period_percent_change
    }
    
    parameters = {key: val for key, val in parameters.items() if val is not None}
    r = call_api('/series', parameters=parameters)
    if r.status_code != 200:
        raise ValueError(f"GFD /series failed with HTTP {r.status_code}: {r.text[:300]}")
    
    try:
        series_data = r.json()
        return series_data
    except Exception as e:
        print(f"ERROR: response was not JSON")
        print(f"Status: {r.status_code}")
        print(f"First 500 chars: {r.text[:500]}")
        print(f"Exception: {e}")
        return None


def _safe_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except Exception:
        return None


def parse_paging_info(search_data: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    Normalize paging metadata returned by /search.

    Observed keys in notebook output:
      - current_page, page_size, total_records, total_pages (all strings)
    """
    paging = (search_data or {}).get("paging_info") or {}
    return {
        "current_page": _safe_int(paging.get("current_page") or paging.get("page")),
        "page_size": _safe_int(paging.get("page_size") or paging.get("pageSize")),
        "total_records": _safe_int(paging.get("total_records") or paging.get("totalResults")),
        "total_pages": _safe_int(paging.get("total_pages") or paging.get("totalPages")),
    }


# DISCOVERY FUNCTIONS

def _iter_search_pages(
    token: str,
    prefix: str,
    *,
    page_size: int,
    sort: str,
    search_type: str = "symbol",
    base_filter: str = "startswith",
    max_pages: Optional[int] = None,
    first_page_data: Optional[Dict[str, Any]] = None,
) -> Iterable[Tuple[int, Dict[str, Any], str]]:
    """
    Generator that iterates through paginated search results for a given prefix.
    """
    total_pages = None
    if first_page_data is not None:
        paging = parse_paging_info(first_page_data)
        total_pages = paging.get("total_pages")

    if total_pages is None:
        page_cap = max_pages if max_pages is not None else 10_000
    else:
        page_cap = total_pages
        if max_pages is not None:
            page_cap = min(page_cap, max_pages)

    seen_fingerprints = set()

    for page in range(1, page_cap + 1):
        if page == 1 and first_page_data is not None:
            search_data = first_page_data
        else:
            search_data, token = safe_gfd_search(
                token,
                search_string=prefix,
                page=str(page),
                pageSize=str(page_size),
                searchType=search_type,
                baseFilter=base_filter,
                sort=sort,
            )

        results = search_data.get("search_results", []) or []
        if not results:
            break

        fingerprint = tuple((r.get("symbol") or "").upper() for r in results[:10])
        if fingerprint in seen_fingerprints:
            break
        seen_fingerprints.add(fingerprint)

        yield page, search_data, token


def _add_company_to_symbols(
    all_symbols: Dict[str, Dict[str, Any]],
    company: Dict[str, Any],
    *,
    filter_fn: Callable[[Dict[str, Any]], bool],
) -> bool:
    symbol = (company.get("symbol") or "").strip()
    if not symbol:
        return False
    if not filter_fn(company):
        return False
    if symbol in all_symbols:
        return False
    all_symbols[symbol] = {
        "symbol": symbol,
        "name": (company.get("description") or company.get("name") or "N/A")[:50],
        "series_id": company.get("series_id"),
    }
    return True


def discover_us_stock_symbols_fixed(
    token: str,
    *,
    max_pages_per_prefix: int = DEFAULT_MAX_PAGES,
    page_size: int = DEFAULT_PAGE_SIZE,
    sort: str = DEFAULT_SEARCH_SORT,
    filter_mode: str = "none",
    prefixes: Optional[Sequence[str]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """
    Discover symbols via fixed paging per first-character prefix.
    
    Strategy:
    1. Search each prefix (A-Z, 0-9, _)
    2. Paginate through results (stable sort recommended: alpha)
    3. Client-side filter to keep only US equities (or other modes)
    4. Stop when: page limit hit OR API indicates end (empty page)
    5. Deduplicate across prefixes
    
    Returns:
        tuple: (discovered_symbols dict, prefix_stats dict)
    """
    
    if prefixes is None:
        prefixes = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") + ["_"]
    all_symbols = {}
    prefix_stats = {}
    requests_made = 0
    
    print("\n" + "=" * 70)
    print("SYMBOL DISCOVERY (FIXED): FILTERED")
    print("=" * 70)
    print(f"Strategy: Paginate {len(prefixes)} prefixes with stable paging (sort={sort})")
    print(f"Max pages per prefix: {max_pages_per_prefix}\n")
    
    start_time = dt.now()

    # No filtering - discover all symbols
    filter_fn = lambda _item: True
    
    for prefix_idx, prefix in enumerate(prefixes, 1):
        print(f"[{prefix_idx:2d}/{len(prefixes)}] Prefix '{prefix}': ", end="", flush=True)
        
        prefix_symbols = set()
        pages_checked = 0
        api_results_total = 0
        filtered_matches = 0

        try:
            first_page, token = safe_gfd_search(
                token,
                search_string=prefix,
                page="1",
                pageSize=str(page_size),
                searchType="symbol",
                baseFilter="startswith",
                sort=sort,
            )
            requests_made += 1
        except Exception as e:
            print(f"Error: {str(e)[:40]}")
            continue

        for page, page_data, token in _iter_search_pages(
            token,
            prefix,
            page_size=page_size,
            sort=sort,
            max_pages=max_pages_per_prefix,
            first_page_data=first_page,
        ):
            results = page_data.get("search_results", []) or []
            requests_made += 0 if page == 1 else 1

            api_results_total += len(results)
            pages_checked = page

            for company in results:
                if filter_fn(company):
                    filtered_matches += 1
                added = _add_company_to_symbols(all_symbols, company, filter_fn=filter_fn)
                if added:
                    prefix_symbols.add(company.get("symbol"))

            time.sleep(0.05)  # Rate limit
        
        # Calculate filter efficiency
        filter_efficiency = (filtered_matches / api_results_total * 100) if api_results_total > 0 else 0
        
        # Report for this prefix
        prefix_stats[prefix] = {
            "pages": pages_checked,
            "api_results": api_results_total,
            "filtered_matches": filtered_matches,
            "unique_symbols": len(prefix_symbols),
            "filter_efficiency": filter_efficiency,
        }
        
    print(f"{len(prefix_symbols):3d} symbols ({filter_efficiency:5.1f}% pass filter, {api_results_total:4d} API, {pages_checked} pages)")
    
    elapsed = (dt.now() - start_time).total_seconds()
    
    print("\n" + "=" * 70)
    print(f"DISCOVERY SUMMARY: US EQUITIES ONLY")
    print("=" * 70)
    print(f"Total unique US stock symbols found: {len(all_symbols):,}")
    print(f"API requests made: {requests_made}")
    print(f"Time elapsed: {elapsed:.1f}s")
    print(f"Rate: {requests_made / (elapsed + 0.001):.1f} requests/sec\n")
    
    return all_symbols, prefix_stats


def discover_us_stock_symbols(token, max_pages_per_prefix=DEFAULT_MAX_PAGES):
    """
    Backwards-compatible wrapper for earlier versions of this repo.

    Prefer `--discovery-strategy adaptive` (or `discover_symbols_adaptive`) for comprehensive runs.
    """
    return discover_us_stock_symbols_fixed(token, max_pages_per_prefix=max_pages_per_prefix)


def discover_symbols_adaptive(
    token: str,
    *,
    page_threshold: int = DEFAULT_MAX_PAGES,
    max_prefix_depth: int = DEFAULT_MAX_PREFIX_DEPTH,
    page_size: int = DEFAULT_PAGE_SIZE,
    sort: str = DEFAULT_SEARCH_SORT,
    root_prefixes: Optional[Sequence[str]] = None,
    prefix_alphabet: str = DEFAULT_PREFIX_ALPHABET,
    filter_mode: str = "none",
    checkpoint_every_leaves: int = 50,
    checkpoint_directory: Optional[str] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """
    Adaptive discovery to avoid deep pagination (which can look like repeats/blank pages).

    It recursively subdivides prefixes until each leaf prefix has <= page_threshold pages
    (based on /search paging_info.total_pages), then fetches all pages for each leaf.
    """
    if root_prefixes is None:
        root_prefixes = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") + ["_"]

    # No filtering - discover all symbols
    filter_fn = lambda _item: True

    all_symbols: Dict[str, Dict[str, Any]] = {}
    prefix_stats: Dict[str, Any] = {}
    requests_made = 0

    queue: List[Tuple[str, int]] = [(p, 0) for p in root_prefixes]
    leaf_prefixes: List[Tuple[str, int, Dict[str, Any]]] = []

    print("\n" + "=" * 70)
    print("SYMBOL DISCOVERY (ADAPTIVE): FILTERED")
    print("=" * 70)
    print(f"Sort: {sort} | Page size: {page_size}")
    print(f"Split threshold: >{page_threshold} pages | Max depth: {max_prefix_depth}")
    print(f"Root prefixes: {len(root_prefixes)}\n")

    start_time = dt.now()

    while queue:
        prefix, depth = queue.pop(0)
        try:
            first_page, token = safe_gfd_search(
                token,
                search_string=prefix,
                page="1",
                pageSize=str(page_size),
                searchType="symbol",
                baseFilter="startswith",
                sort=sort,
            )
            requests_made += 1
        except Exception as e:
            prefix_stats[prefix] = {"error": str(e), "depth": depth}
            continue

        paging = parse_paging_info(first_page)
        total_pages = paging.get("total_pages")
        total_records = paging.get("total_records")

        should_split = (
            total_pages is not None
            and total_pages > page_threshold
            and depth < max_prefix_depth
        )

        prefix_stats[prefix] = {
            "depth": depth,
            "total_pages": total_pages,
            "total_records": total_records,
            "split": should_split,
        }

        if should_split:
            # Preserve symbols exactly equal to the prefix (e.g., "A", "AA") which
            # would otherwise be lost when we only search children like "AA", "AB", ...
            try:
                exact, token = safe_gfd_search(
                    token,
                    search_string=prefix,
                    page="1",
                    pageSize="10",
                    searchType="symbol",
                    baseFilter="exactmatch",
                    sort=sort,
                )
                requests_made += 1
                for company in exact.get("search_results", []) or []:
                    _add_company_to_symbols(all_symbols, company, filter_fn=filter_fn)
            except Exception:
                pass

            for ch in prefix_alphabet:
                queue.append((prefix + ch, depth + 1))
        else:
            leaf_prefixes.append((prefix, depth, first_page))

        time.sleep(0.02)

    # Fetch leaf prefixes fully
    leaf_total = len(leaf_prefixes)
    for idx, (prefix, depth, first_page) in enumerate(leaf_prefixes, 1):
        paging = parse_paging_info(first_page)
        total_pages = paging.get("total_pages")
        if total_pages is None:
            # Defensive cap if paging_info is missing/unparseable.
            total_pages = page_threshold
            prefix_stats[prefix]["total_pages_missing"] = True

        raw_results = 0
        matched = 0
        added = 0
        pages_fetched = 0

        for page, page_data, token in _iter_search_pages(
            token,
            prefix,
            page_size=page_size,
            sort=sort,
            max_pages=total_pages,  # fetch all, as advertised by paging_info (or capped)
            first_page_data=first_page,
        ):
            results = page_data.get("search_results", []) or []
            if not results:
                break

            pages_fetched = page
            raw_results += len(results)
            for company in results:
                if filter_fn(company):
                    matched += 1
                if _add_company_to_symbols(all_symbols, company, filter_fn=filter_fn):
                    added += 1

            time.sleep(0.05)

        prefix_stats[prefix].update(
            {
                "leaf": True,
                "leaf_index": idx,
                "leaf_total": leaf_total,
                "pages_fetched": pages_fetched,
                "raw_results": raw_results,
                "matched": matched,
                "added": added,
            }
        )

        if idx % 25 == 0 or idx == leaf_total:
            print(f"[leaf {idx}/{leaf_total}] {prefix}: +{added} (matched {matched}, raw {raw_results}, pages {pages_fetched})")

        # Periodic checkpoint so you don't lose progress if the run is interrupted.
        if checkpoint_every_leaves and idx % checkpoint_every_leaves == 0:
            checkpoint_payload = {
                "symbols": all_symbols,
                "stats": prefix_stats,
                "timestamp": dt.now().isoformat(),
                "progress": {"leaf_index": idx, "leaf_total": leaf_total, "last_prefix": prefix},
            }
            out_dir = checkpoint_directory or MANIFEST_DIR
            try:
                os.makedirs(out_dir, exist_ok=True)
                ckpt_path = os.path.join(out_dir, f"discovery_checkpoint_{dt.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(ckpt_path, "w") as f:
                    json.dump(checkpoint_payload, f, indent=2)
                print(f"        [checkpoint saved] {ckpt_path}")
            except Exception:
                # Fallback to local ./responses if Dropbox isn't available.
                ckpt_path = write_json_to_file("discovery_checkpoint", checkpoint_payload, directory="./responses/")
                print(f"        [checkpoint saved] {ckpt_path}")

    elapsed = (dt.now() - start_time).total_seconds()
    print("\n" + "=" * 70)
    print("DISCOVERY SUMMARY")
    print("=" * 70)
    print(f"Total discovered (post-filter) symbols: {len(all_symbols):,}")
    print(f"API requests made (approx): {requests_made}")
    print(f"Time elapsed: {elapsed:.1f}s\n")

    return all_symbols, prefix_stats


def probe_search_pagination(
    token: str,
    *,
    prefix: str,
    pages: int = 25,
    page_size: int = DEFAULT_PAGE_SIZE,
    sort: str = DEFAULT_SEARCH_SORT,
    search_type: str = "symbol",
    base_filter: str = "startswith",
) -> str:
    """
    Diagnostic helper: fetches a few pages for a prefix and records paging_info
    plus basic duplicate/repeat signals (useful when pages look blank/repeating).
    """
    report: Dict[str, Any] = {
        "ran_at": dt.now().isoformat(),
        "request": {
            "searchString": prefix,
            "searchType": search_type,
            "baseFilter": base_filter,
            "sort": sort,
            "pageSize": page_size,
            "pages_requested": pages,
        },
        "paging_info": None,
        "pages": [],
    }

    first, token = safe_gfd_search(
        token,
        search_string=prefix,
        page="1",
        pageSize=str(page_size),
        searchType=search_type,
        baseFilter=base_filter,
        sort=sort,
    )
    report["paging_info"] = parse_paging_info(first)
    total_pages = report["paging_info"].get("total_pages")
    if total_pages is not None:
        pages = min(pages, total_pages)

    seen = set()
    prev_syms: Optional[List[str]] = None

    for page in range(1, pages + 1):
        if page == 1:
            data = first
        else:
            data, token = safe_gfd_search(
                token,
                search_string=prefix,
                page=str(page),
                pageSize=str(page_size),
                searchType=search_type,
                baseFilter=base_filter,
                sort=sort,
            )
        results = data.get("search_results", []) or []
        syms = [(r.get("symbol") or "").upper() for r in results]
        fp = tuple(syms[:25])
        repeated = fp in seen
        seen.add(fp)

        entry = {
            "page": page,
            "results_len": len(results),
            "first_symbol": syms[0] if syms else None,
            "last_symbol": syms[-1] if syms else None,
            "unique_symbols": len(set(syms)),
            "same_as_prev_page": (prev_syms == syms) if prev_syms is not None else False,
            "repeated_fingerprint": repeated,
        }
        report["pages"].append(entry)

        if not results or repeated:
            break
        prev_syms = syms
        time.sleep(0.05)

    out = write_json_to_file(
        f"pagination_probe__{prefix}",
        report,
        directory="./responses/",
    )
    return out

# BATCH SCRAPING FUNCTIONS

def check_already_scraped(symbols_to_check):
    """
    Scan SERIES_DIR for already-scraped symbols.
    
    Returns:
        set: Symbols that have already been scraped
    """
    already_scraped = set()
    
    ensure_output_dirs()

    if os.path.exists(SERIES_DIR):
        # Look for series files
        for file_pattern in ["*.json", "*/*.json"]:
            for filepath in glob.glob(os.path.join(SERIES_DIR, file_pattern)):
                filename = os.path.basename(filepath)
                # Extract symbol from filename (assumes format: series__SYMBOL__*.json)
                if "series__" in filename:
                    parts = filename.split("__")
                    if len(parts) >= 2:
                        symbol = parts[1].upper()
                        already_scraped.add(symbol)
    
    return already_scraped


def batch_scrape_series(token, companies_dict, batch_name="discovered_symbols",
                       start_date=DEFAULT_START_DATE, end_date=DEFAULT_END_DATE,
                       periodicity=DEFAULT_PERIODICITY):
    """
    Batch scrape time series data for discovered symbols.
    
    Args:
        token: Authentication token
        companies_dict: Dict of {symbol: metadata}
        batch_name: Name for this batch run
        start_date: Start date for time series
        end_date: End date for time series
        periodicity: daily, monthly, quarterly, etc.
    
    Returns:
        dict: Batch results with completed/failed counts
    """
    
    print("\n" + "=" * 80)
    print("SERIES DATA COLLECTION: DISCOVERED SYMBOLS")
    print("=" * 80)

    ensure_output_dirs()
    
    # Step 1: Check what's already saved
    print("\nStep 1: Scanning for already-scraped series data...")
    print("-" * 80)
    
    already_scraped = check_already_scraped(set(companies_dict.keys()))
    
    print(f"Found {len(already_scraped)} symbols with existing series data:")
    if already_scraped:
        sample = sorted(list(already_scraped))[:20]
        print(f"  Sample: {', '.join(sample)}")
        if len(already_scraped) > 20:
            print(f"  ... and {len(already_scraped) - 20} more")
    
    # Step 2: Determine which symbols need to be scraped
    print("\nStep 2: Comparing with discovered symbols...")
    print("-" * 80)
    
    discovered_set = set(companies_dict.keys())
    to_scrape = discovered_set - already_scraped
    
    print(f"Discovered symbols: {len(discovered_set)}")
    print(f"Already scraped: {len(already_scraped)}")
    print(f"Need to scrape: {len(to_scrape)}")
    
    if len(to_scrape) == 0:
        print("\n⚠️  All discovered symbols already have series data!")
        return {"completed": {}, "failed": {}, "total_records": 0}
    
    print(f"\n✓ Ready to scrape {len(to_scrape)} new symbols")
    
    # Step 3: Create batch config
    print("\nStep 3: Preparing batch configuration...")
    print("-" * 80)
    
    companies_to_scrape = {
        symbol: {
            "symbol": symbol,
            "name": companies_dict[symbol].get("name", "N/A"),
            "series_id": companies_dict[symbol].get("series_id"),
        }
        for symbol in sorted(to_scrape)
    }
    
    print(f"Created batch config for {len(companies_to_scrape)} symbols")
    if len(companies_to_scrape) > 0:
        print(f"Sample symbols to scrape: {', '.join(sorted(companies_to_scrape.keys())[:20])}")
    
    # Step 4: Safety check summary
    print("\nStep 4: SAFETY CHECK - Summary before execution")
    print("-" * 80)
    output_subdir = os.path.join(SERIES_DIR, f"discovered_{dt.now().strftime('%Y%m%d')}")
    os.makedirs(output_subdir, exist_ok=True)
    
    print(f"OUTPUT DIRECTORY: {output_subdir}")
    print(f"TOTAL SYMBOLS TO SCRAPE: {len(companies_to_scrape)}")
    print(f"OVERLAP CHECK: ✓ Verified - will skip {len(already_scraped)} already-saved symbols")
    print(f"DEDUPLICATION: ✓ Enabled - each symbol appears only once")
    print(f"\nProceeding with batch scraping...\n")
    
    # Save batch config
    batch_config_path = os.path.join(output_subdir, "batch_config.json")
    with open(batch_config_path, "w") as f:
        json.dump({
            "total_discovered": len(discovered_set),
            "already_scraped": len(already_scraped),
            "to_scrape": len(companies_to_scrape),
            "symbols_to_scrape": sorted(companies_to_scrape.keys()),
            "timestamp": dt.now().isoformat(),
        }, f, indent=2)
    
    # Step 5: Execute batch scraping
    print("=" * 80)
    print("EXECUTING BATCH SCRAPE FOR DISCOVERED SYMBOLS")
    print("=" * 80)
    
    batch_results: Dict[str, Any] = {
        "timestamp": dt.now().isoformat(),
        "batch_name": batch_name,
        "config": {
            "start_date": start_date,
            "end_date": end_date,
            "periodicity": periodicity
        },
        "completed": {},
        "failed": {},
        "total_records": 0
    }
    
    for i, symbol in enumerate(sorted(companies_to_scrape.keys()), 1):
        try:
            print(f"\n[{i}/{len(companies_to_scrape)}] [{symbol}] Fetching...", end=" ", flush=True)
            
            # Refresh token every 50 requests (30 min lifetime)
            if i % 50 == 0:
                auth_json = gfd_auth(GFD_USERNAME, GFD_PASSWORD)
                token = auth_json['token'].strip('"')
                print(f"\n        [TOKEN REFRESHED] ", end="")
            
            # Fetch series data (auto-refresh token if it expires)
            series_data, token = safe_gfd_series(
                token,
                seriesName=symbol,
                periodicity=periodicity,
                closeOnly="true",
                totalReturn="true",
                startDate=start_date if start_date else None,
                endDate=end_date if end_date else None
            )
            
            if series_data is None:
                raise Exception("API returned None")
            
            price_data = series_data.get("price_data", [])
            num_records = len(price_data)
            
            if num_records == 0:
                raise Exception("No price data found")
            
            # Save to Dropbox
            if start_date and end_date:
                date_tag = f"{start_date.replace('/', '')}-{end_date.replace('/', '')}"
                filename = f"series__{symbol}__{periodicity}__{date_tag}.json"
            elif start_date and not end_date:
                filename = f"series__{symbol}__{periodicity}__from_{start_date.replace('/', '')}.json"
            elif (not start_date) and end_date:
                filename = f"series__{symbol}__{periodicity}__to_{end_date.replace('/', '')}.json"
            else:
                filename = f"series__{symbol}__{periodicity}__full.json"
            filepath = os.path.join(output_subdir, filename)
            
            series_data_to_save = _strip_token_fields(series_data)
            with open(filepath, "w") as f:
                json.dump(series_data_to_save, f, indent=2)
            
            # Log success
            batch_results["completed"][symbol] = {
                "status": "success",
                "records": num_records,
                "filepath": filepath,
                "scraped_at": dt.now().isoformat()
            }
            batch_results["total_records"] += num_records
            
            print(f"✓ {num_records} records")
            
        except Exception as e:
            batch_results["failed"][symbol] = {
                "error": str(e),
                "attempted_at": dt.now().isoformat()
            }
            print(f"✗ ERROR: {str(e)[:50]}")
        
        # Save progress every 10 symbols
        if i % 10 == 0:
            manifest_path = os.path.join(MANIFEST_DIR, f"batch_{batch_name}_{dt.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(manifest_path, "w") as f:
                json.dump(batch_results, f, indent=2)
            print(f"        [Progress saved]")
    
    # Final save
    manifest_path = os.path.join(MANIFEST_DIR, f"batch_{batch_name}_{dt.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(manifest_path, "w") as f:
        json.dump(batch_results, f, indent=2)
    
    print("\n" + "=" * 80)
    print("BATCH SCRAPING COMPLETE!")
    print("=" * 80)
    print(f"\n📊 RESULTS:")
    print(f"   ✓ Successfully scraped: {len(batch_results['completed'])} symbols")
    print(f"   ✗ Failed: {len(batch_results['failed'])} symbols")
    print(f"   📈 Total time series records: {batch_results['total_records']:,}")
    print(f"   💾 Output directory: {output_subdir}")
    print(f"   📝 Manifest: {manifest_path}")
    
    # Safety check
    newly_scraped = set(batch_results['completed'].keys())
    overlap = newly_scraped & already_scraped
    
    print(f"\n🔒 SAFETY CHECK - Overlap Verification:")
    if len(overlap) > 0:
        print(f"   ⚠️  WARNING: {len(overlap)} symbols were re-scraped!")
        print(f"       Overlapping symbols: {', '.join(sorted(list(overlap)[:5]))}")
    else:
        print(f"   ✓ No overlap detected - all scraped symbols are new")
    
    print(f"\n✓ Pipeline complete. Discovered symbols with series data: {len(already_scraped) + len(newly_scraped)}\n")
    
    return batch_results


# MAIN EXECUTION

def main():
    """Main execution flow."""
    
    parser = argparse.ArgumentParser(
        description="GFD Symbol Discovery and Time Series Scraper"
    )
    parser.add_argument(
        "--discovery-only",
        action="store_true",
        help="Only run discovery, don't scrape"
    )
    parser.add_argument(
        "--probe-prefix",
        type=str,
        help="Diagnostics: probe pagination for a single prefix (writes ./responses/* and exits)"
    )
    parser.add_argument(
        "--probe-pages",
        type=int,
        default=25,
        help="Diagnostics: number of pages to probe (default: 25)"
    )
    parser.add_argument(
        "--test-symbol",
        type=str,
        help="Diagnostics: fetch /series for a single symbol (consumes 1 download if successful) and exit"
    )
    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Only scrape, skip discovery (requires existing discovered_symbols.json)"
    )
    parser.add_argument(
        "--load-discovery",
        type=str,
        help="Load discovery from JSON file instead of running discovery"
    )
    parser.add_argument(
        "--discovery-strategy",
        choices=["fixed", "adaptive"],
        default=DEFAULT_DISCOVERY_STRATEGY,
        help=f"Discovery strategy (default: {DEFAULT_DISCOVERY_STRATEGY})"
    )
    parser.add_argument(
        "--filter-mode",
        choices=["us_equity", "equity", "none"],
        default="none",
        help="Discovery filter applied to /search results (default: none)"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Fixed mode: max pages per 1-char prefix (default: {DEFAULT_MAX_PAGES})"
    )
    parser.add_argument(
        "--page-threshold",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Adaptive mode: split prefixes when total_pages exceeds this (default: {DEFAULT_MAX_PAGES})"
    )
    parser.add_argument(
        "--max-prefix-depth",
        type=int,
        default=DEFAULT_MAX_PREFIX_DEPTH,
        help=f"Adaptive mode: maximum prefix subdivision depth (default: {DEFAULT_MAX_PREFIX_DEPTH})"
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Search page size (default: {DEFAULT_PAGE_SIZE})"
    )
    parser.add_argument(
        "--search-sort",
        choices=["pop", "alpha"],
        default=DEFAULT_SEARCH_SORT,
        help=f"Search sorting for stable pagination (default: {DEFAULT_SEARCH_SORT})"
    )
    parser.add_argument(
        "--checkpoint-every-leaves",
        type=int,
        default=DEFAULT_CHECKPOINT_EVERY_LEAVES,
        help=f"Adaptive discovery: save a checkpoint every N leaf prefixes (default: {DEFAULT_CHECKPOINT_EVERY_LEAVES})"
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=None,
        help="Adaptive discovery: directory for checkpoints (default: manifest dir; falls back to ./responses/)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=DEFAULT_START_DATE,
        help="Start date for scraping (MM/DD/YYYY). Omit for earliest available."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=DEFAULT_END_DATE,
        help="End date for scraping (MM/DD/YYYY). Omit for latest available."
    )
    parser.add_argument(
        "--periodicity",
        type=str,
        default=DEFAULT_PERIODICITY,
        help=f"Series periodicity (default: {DEFAULT_PERIODICITY})"
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 80)
    print("GFD SYMBOL DISCOVERY AND TIME SERIES SCRAPER")
    print("=" * 80)
    print(f"Start time: {dt.now()}")
    print(f"Output directory: {SERIES_DIR}")
    print(f"Manifest directory: {MANIFEST_DIR}")
    print()

    ensure_output_dirs()
    
    discovered_symbols = None
    
    # STEP 1: Authenticate
    print("\nSTEP 1: AUTHENTICATE")
    print("-" * 80)
    auth_json = gfd_auth(GFD_USERNAME, GFD_PASSWORD)
    token = auth_json['token'].strip('"')
    print(f"✓ Authentication successful\n")

    # Optional: run pagination probe and exit
    if args.probe_prefix:
        out = probe_search_pagination(
            token,
            prefix=args.probe_prefix,
            pages=args.probe_pages,
            page_size=args.page_size,
            sort=args.search_sort,
        )
        print(f"✓ Probe saved to: {out}")
        return

    # Optional: fetch one symbol series and exit (quota/entitlement sanity check)
    if args.test_symbol:
        symbol = args.test_symbol.strip()
        print("\n" + "=" * 80)
        print(f"TEST SYMBOL SERIES: {symbol}")
        print("=" * 80)
        try:
            series_data, token = safe_gfd_series(
                token,
                seriesName=symbol,
                periodicity=args.periodicity,
                closeOnly="true",
                totalReturn="true",
                startDate=args.start_date if args.start_date else None,
                endDate=args.end_date if args.end_date else None,
            )
            if series_data is None:
                print("✗ API returned None")
                return
            price = series_data.get("price_data") or []
            info = (series_data.get("data_information") or [{}])[0]
            print(f"Records: {len(price)}")
            if price:
                print(f"Date range: {price[0].get('date')} -> {price[-1].get('date')}")
            print(f"Data notes: {(info.get('data notes') or info.get('data_notes') or '')[:200]}")
            print(f"Download status: {series_data.get('download_status')}")
        except Exception as e:
            print(f"✗ ERROR: {e}")
        return
    
    # STEP 2: Discover symbols (or load from file)
    print("\nSTEP 2: DISCOVER SYMBOLS (or load from file)")
    print("-" * 80)
    
    if args.load_discovery:
        print(f"Loading discovery from: {args.load_discovery}")
        with open(args.load_discovery, 'r') as f:
            loaded = json.load(f)

        # Support both formats:
        #  1) {"symbols": {...}, "stats": {...}, "timestamp": "..."} (this script's saved output)
        #  2) {...} where keys are symbols
        #  3) ["AAPL", "MSFT", ...]
        if isinstance(loaded, dict) and "symbols" in loaded and isinstance(loaded["symbols"], dict):
            discovered_symbols = loaded["symbols"]
        elif isinstance(loaded, dict):
            discovered_symbols = loaded
        elif isinstance(loaded, list):
            discovered_symbols = {str(s): {"symbol": str(s), "name": "N/A", "series_id": None} for s in loaded}
        else:
            raise ValueError(f"Unsupported discovery file format: {type(loaded).__name__}")

        print(f"✓ Loaded {len(discovered_symbols)} symbols from file\n")
    else:
        try:
            if args.discovery_strategy == "fixed":
                discovered_symbols, prefix_stats = discover_us_stock_symbols_fixed(
                    token,
                    max_pages_per_prefix=args.max_pages,
                    page_size=args.page_size,
                    sort=args.search_sort,
                    filter_mode=args.filter_mode,
                )
            else:
                discovered_symbols, prefix_stats = discover_symbols_adaptive(
                    token,
                    page_threshold=args.page_threshold,
                    max_prefix_depth=args.max_prefix_depth,
                    page_size=args.page_size,
                    sort=args.search_sort,
                    filter_mode=args.filter_mode,
                    checkpoint_every_leaves=args.checkpoint_every_leaves,
                    checkpoint_directory=args.checkpoint_dir,
                )
        except Exception as e:
            print(f"\n✗ Discovery failed: {e}")
            print("  Checkpoints may exist in your manifest directory or ./responses/.")
            raise
        
        # Save discovery results
        discovery_file = os.path.join(MANIFEST_DIR, f"discovery_{dt.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(discovery_file, "w") as f:
            json.dump({
                "symbols": discovered_symbols,
                "stats": prefix_stats,
                "timestamp": dt.now().isoformat()
            }, f, indent=2)
        print(f"✓ Discovery saved to: {discovery_file}\n")
    
    # Exit if discovery-only mode
    if args.discovery_only:
        print(f"\nDiscovery complete. Exiting (--discovery-only flag set).")
        return
    
    # STEP 3: Check what's already scraped (happens in batch_scrape_series)
    # STEP 4: Batch scrape remaining symbols
    print("\nSTEP 3-4: CHECK SCRAPED DATA AND BATCH SCRAPE")
    print("-" * 80)

    # Discovery can take long enough to expire the token; refresh before the scrape run.
    try:
        token = _refresh_token()
    except Exception:
        pass
    
    batch_results = batch_scrape_series(
        token,
        discovered_symbols,
        batch_name=f"discovered_{dt.now().strftime('%Y%m%d_%H%M%S')}",
        start_date=args.start_date,
        end_date=args.end_date,
        periodicity=args.periodicity
    )
    
    # STEP 5: Save final manifest (done in batch_scrape_series)
    print("\nSTEP 5: FINAL MANIFEST")
    print("-" * 80)
    
    final_manifest_path = os.path.join(MANIFEST_DIR, f"pipeline_final_{dt.now().strftime('%Y%m%d_%H%M%S')}.json")
    with open(final_manifest_path, "w") as f:
        json.dump({
            "total_discovered": len(discovered_symbols),
            "total_successfully_scraped": len(batch_results.get('completed', {})),
            "total_failed": len(batch_results.get('failed', {})),
            "total_records": batch_results.get('total_records', 0),
            "timestamp": dt.now().isoformat(),
            "output_directory": SERIES_DIR,
            "manifest_directory": MANIFEST_DIR
        }, f, indent=2)
    print(f"✓ Final manifest saved to: {final_manifest_path}\n")
    
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETE")
    print("=" * 80)
    print(f"End time: {dt.now()}")
    print(f"Data location: {SERIES_DIR}")
    print(f"Manifests: {MANIFEST_DIR}")
    print()


if __name__ == "__main__":
    main()
