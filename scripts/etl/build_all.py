from __future__ import annotations

import io
import json
import hashlib
import math
import os
import re
import time
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any
from urllib.parse import quote, urljoin

import numpy as np
import pandas as pd
import requests
import tushare as ts
from bs4 import BeautifulSoup
from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data_sources" / "raw"
INPUT_DATA_DIR = ROOT / "input_data"
PUBLIC_DATA_DIR = ROOT / "public" / "data"
TUSHARE_CACHE_DIR = ROOT / "data_sources" / "cache" / "tushare"
TUSHARE_CACHE_META_DIR = TUSHARE_CACHE_DIR / "_meta"
TUSHARE_CACHE_STATE_PATH = TUSHARE_CACHE_META_DIR / "cache_state.json"
HTTP_CACHE_DIR = ROOT / "data_sources" / "cache" / "http"
CONFIG_PATH = RAW_DIR / "sector_board_config.json"
PB_3Y_DAYS = 365 * 3 + 30
PB_5Y_DAYS = 365 * 5 + 45
DEFAULT_STALE_AFTER_MINUTES = 24 * 60
CHART_POINTS = 252
CHART_LOOKBACK_DAYS = 366
ROE_HISTORY_QUARTERS = 20
PB_ROE_SCATTER_LOOKBACK_DAYS = 365 * 6
BENCHMARK_CODE = "000906.SH"
BENCHMARK_LABEL = "中证800"
HTTP_TIMEOUT = 60
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    )
}
STATS_SEARCH_URL = "https://api.so-gov.cn/query/s"
STATS_SITE_CODE = "bm36000002"
PBC_SURVEY_LIST_URL = "https://www.pbc.gov.cn/diaochatongjisi/116219/116227/index.html"
PBC_SURVEY_BASE_URL = "https://www.pbc.gov.cn/diaochatongjisi/116219/116227/"
HOUSEHOLD_CONSUMPTION_START_YEAR = 2019
MARKET_TRACKING_START_MONTH = "202106"
MARKET_TRACKING_PATH = PUBLIC_DATA_DIR / "market-tracking-board.json"
REAL_ESTATE_HIGH_FREQUENCY_PATH = PUBLIC_DATA_DIR / "real-estate-high-frequency-board.json"
REAL_ESTATE_HIGH_FREQUENCY_START_YEAR = 2019
REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS = 3
REAL_ESTATE_HIGH_FREQUENCY_NEW_HOME_ROLLING_YEARS = 5
INCOME_CHART_START_YEAR = 2017
BINGSHAN_INDEX_PATH = INPUT_DATA_DIR / "bingshan_index.xlsx"
HOUSEHOLD_ASSET_PATH = INPUT_DATA_DIR / "household_asset.xlsx"
NEW_HOUSE_PATH = INPUT_DATA_DIR / "new_house.xlsx"
CONSUMER_MARKET_INDUSTRY_CODES = {
    "801120.SI": "食品饮料",
    "801010.SI": "农林牧渔",
    "801980.SI": "美容护理",
    "801110.SI": "家用电器",
    "801200.SI": "商贸零售",
    "801210.SI": "社会服务",
    "801140.SI": "轻工制造",
    "801130.SI": "纺织服饰",
}
CROWDING_BENCHMARK_CODE = "000985.CSI"
CROWDING_HISTORY_LOOKBACK_DAYS = 365 * 7
CROWDING_PERCENTILE_WINDOW = 1250
CROWDING_EXCESS_NAV_BIAS_WINDOWS = (40, 60, 120)
CROWDING_EXCESS_MOMENTUM_WINDOWS = (20, 40, 60)
CROWDING_TURNOVER_WINDOWS = (5, 10, 20, 40, 60)
CROWDING_TURNOVER_BIAS_WINDOWS = (120, 250)
TUSHARE_TIMEOUT_SECONDS = int(os.environ.get("TUSHARE_TIMEOUT_SECONDS", "90"))
TUSHARE_MAX_RETRIES = int(os.environ.get("TUSHARE_MAX_RETRIES", "3"))
TUSHARE_RETRY_BACKOFF_SECONDS = (2, 5, 10)
TUSHARE_RECENT_BACKFILL_DAYS = 20
TUSHARE_RECENT_ROE_QUARTERS = 8
TUSHARE_RECENT_BROKER_MONTHS = 3
STATS_FETCH_RETRIES = 3
STATS_RETRY_BACKOFF_SECONDS = (2, 4, 8)

CITY_TIER_MAP = {
    "北京": "一线",
    "上海": "一线",
    "广州": "一线",
    "深圳": "一线",
    "天津": "二线",
    "石家庄": "二线",
    "太原": "二线",
    "呼和浩特": "二线",
    "沈阳": "二线",
    "大连": "二线",
    "长春": "二线",
    "哈尔滨": "二线",
    "南京": "二线",
    "杭州": "二线",
    "宁波": "二线",
    "合肥": "二线",
    "福州": "二线",
    "厦门": "二线",
    "南昌": "二线",
    "济南": "二线",
    "青岛": "二线",
    "郑州": "二线",
    "武汉": "二线",
    "长沙": "二线",
    "南宁": "二线",
    "海口": "二线",
    "重庆": "二线",
    "成都": "二线",
    "贵阳": "二线",
    "昆明": "二线",
    "西安": "二线",
    "兰州": "二线",
    "银川": "二线",
    "乌鲁木齐": "二线",
    "唐山": "三线",
    "秦皇岛": "三线",
    "包头": "三线",
    "丹东": "三线",
    "锦州": "三线",
    "吉林": "三线",
    "牡丹江": "三线",
    "无锡": "三线",
    "扬州": "三线",
    "徐州": "三线",
    "温州": "三线",
    "金华": "三线",
    "蚌埠": "三线",
    "安庆": "三线",
    "泉州": "三线",
    "九江": "三线",
    "赣州": "三线",
    "烟台": "三线",
    "济宁": "三线",
    "洛阳": "三线",
    "平顶山": "三线",
    "宜昌": "三线",
    "襄阳": "三线",
    "岳阳": "三线",
    "常德": "三线",
    "韶关": "三线",
    "湛江": "三线",
    "惠州": "三线",
    "北海": "三线",
    "三亚": "三线",
    "泸州": "三线",
    "南充": "三线",
    "遵义": "三线",
    "大理": "三线",
}

_CACHE_STATE: dict[str, Any] | None = None
_SW_DAILY_RUNTIME_CACHE: dict[str, pd.DataFrame] = {}
_INDEX_DAILY_RUNTIME_CACHE: dict[str, pd.DataFrame] = {}
_INDEX_MEMBER_RUNTIME_CACHE: dict[str, pd.DataFrame] = {}
_INDEX_CLASSIFY_RUNTIME_CACHE: dict[str, pd.DataFrame] = {}
_BROKER_RECOMMEND_RUNTIME_CACHE: dict[tuple[str, str], pd.DataFrame] = {}
_ROE_RUNTIME_CACHE: dict[str, pd.DataFrame] = {}


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def load_cache_state() -> dict[str, Any]:
    global _CACHE_STATE
    if _CACHE_STATE is None:
        if TUSHARE_CACHE_STATE_PATH.exists():
            _CACHE_STATE = load_json(TUSHARE_CACHE_STATE_PATH)
        else:
            _CACHE_STATE = {"updated_at": None, "datasets": {}}
    return _CACHE_STATE


def save_cache_state() -> None:
    state = load_cache_state()
    state["updated_at"] = now_iso()
    ensure_dir(TUSHARE_CACHE_META_DIR)
    write_json(TUSHARE_CACHE_STATE_PATH, state)


def update_cache_state(
    dataset: str,
    key: str,
    status: str,
    *,
    message: str | None = None,
    **details: Any,
) -> None:
    state = load_cache_state()
    dataset_state = state.setdefault("datasets", {}).setdefault(dataset, {})
    entry = dataset_state.setdefault(key, {})
    attempt_time = now_iso()
    entry["status"] = status
    entry["last_attempt_at"] = attempt_time
    if status == "success":
        entry["last_success_at"] = attempt_time
    if message:
        entry["last_message"] = message
    for detail_key, detail_value in details.items():
        if detail_value is not None:
            entry[detail_key] = detail_value
    save_cache_state()


def load_cached_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def save_cached_csv(path: Path, frame: pd.DataFrame) -> None:
    ensure_dir(path.parent)
    frame.to_csv(path, index=False, encoding="utf-8-sig")


def is_retryable_tushare_error(exc: Exception) -> bool:
    if isinstance(exc, requests.RequestException):
        return True
    message = str(exc)
    retryable_markers = (
        "Read timed out",
        "HTTPConnectionPool",
        "Max retries exceeded",
        "Connection aborted",
        "Connection reset",
        "temporarily unavailable",
    )
    return any(marker in message for marker in retryable_markers)


def call_tushare_with_retry(fetcher: Any, *, dataset: str, key: str) -> Any:
    attempts = max(1, TUSHARE_MAX_RETRIES)
    for attempt in range(attempts):
        try:
            return fetcher()
        except Exception as exc:
            if attempt >= attempts - 1 or not is_retryable_tushare_error(exc):
                raise
            wait_seconds = TUSHARE_RETRY_BACKOFF_SECONDS[min(attempt, len(TUSHARE_RETRY_BACKOFF_SECONDS) - 1)]
            print(
                f"[tushare:{dataset}] {key} request failed on attempt {attempt + 1}/{attempts}: "
                f"{exc}; retrying in {wait_seconds}s."
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"Unexpected retry state for {dataset}:{key}")


def normalize_fetched_at_column(frame: pd.DataFrame, fetched_at: str | None = None) -> pd.DataFrame:
    normalized = frame.copy()
    stamp = fetched_at or now_iso()
    if "fetched_at" not in normalized.columns:
        normalized["fetched_at"] = stamp
    else:
        normalized["fetched_at"] = normalized["fetched_at"].fillna("").astype(str).str.strip()
        normalized.loc[normalized["fetched_at"].eq(""), "fetched_at"] = stamp
    return normalized


def merge_date_segments(segments: list[tuple[date, date]]) -> list[tuple[date, date]]:
    valid_segments = sorted((start, end) for start, end in segments if start <= end)
    if not valid_segments:
        return []

    merged: list[tuple[date, date]] = [valid_segments[0]]
    for start, end in valid_segments[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end + timedelta(days=1):
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def merge_cached_frames(
    frames: list[pd.DataFrame],
    *,
    key_columns: list[str],
    sort_columns: list[str],
) -> pd.DataFrame:
    usable_frames = [frame.copy() for frame in frames if frame is not None and not frame.empty]
    if not usable_frames:
        for frame in frames:
            if frame is not None:
                return frame.head(0).copy()
        return pd.DataFrame()

    combined = pd.concat(usable_frames, ignore_index=True)
    sort_by = [column for column in [*sort_columns, "fetched_at"] if column in combined.columns]
    if sort_by:
        combined.sort_values(sort_by, inplace=True)
    combined.drop_duplicates(subset=key_columns, keep="last", inplace=True)
    if sort_by:
        combined.sort_values(sort_by[:-1] if sort_by[-1] == "fetched_at" else sort_by, inplace=True)
    combined.reset_index(drop=True, inplace=True)
    return combined


def normalize_sw_daily_frame(frame: pd.DataFrame, fetched_at: str | None = None) -> pd.DataFrame:
    columns = ["trade_date", "trade_date_dt", "close", "pct_change", "pb", "amount", "float_mv", "fetched_at"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    normalized = frame.copy()
    for column in ["trade_date", "close", "pct_change", "pb", "amount", "float_mv"]:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized["trade_date"] = normalized["trade_date"].map(to_yyyymmdd)
    normalized = normalized[normalized["trade_date"].ne("")].copy()
    if normalized.empty:
        return pd.DataFrame(columns=columns)
    normalized["trade_date_dt"] = pd.to_datetime(normalized["trade_date"], format="%Y%m%d", errors="coerce")
    normalized = normalized[normalized["trade_date_dt"].notna()].copy()
    for column in ["close", "pct_change", "pb", "amount", "float_mv"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized = normalize_fetched_at_column(normalized, fetched_at)
    normalized.sort_values(["trade_date_dt", "fetched_at"], inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized[columns]


def normalize_index_daily_frame(frame: pd.DataFrame, fetched_at: str | None = None) -> pd.DataFrame:
    columns = ["trade_date", "trade_date_dt", "close", "fetched_at"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    normalized = frame.copy()
    for column in ["trade_date", "close"]:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized["trade_date"] = normalized["trade_date"].map(to_yyyymmdd)
    normalized = normalized[normalized["trade_date"].ne("")].copy()
    if normalized.empty:
        return pd.DataFrame(columns=columns)
    normalized["trade_date_dt"] = pd.to_datetime(normalized["trade_date"], format="%Y%m%d", errors="coerce")
    normalized = normalized[normalized["trade_date_dt"].notna()].copy()
    normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
    normalized = normalize_fetched_at_column(normalized, fetched_at)
    normalized.sort_values(["trade_date_dt", "fetched_at"], inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized[columns]


def normalize_roe_frame(frame: pd.DataFrame, fetched_at: str | None = None) -> pd.DataFrame:
    columns = ["ts_code", "end_date", "roe_dt", "roe", "fetched_at"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    normalized = frame.copy()
    for column in ["ts_code", "end_date", "roe_dt", "roe"]:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized["ts_code"] = normalized["ts_code"].fillna("").astype(str).str.strip()
    normalized["end_date"] = normalized["end_date"].map(to_yyyymmdd)
    normalized = normalized[normalized["ts_code"].ne("") & normalized["end_date"].ne("")].copy()
    normalized["roe_dt"] = pd.to_numeric(normalized["roe_dt"], errors="coerce")
    normalized["roe"] = pd.to_numeric(normalized["roe"], errors="coerce")
    normalized = normalize_fetched_at_column(normalized, fetched_at)
    normalized.sort_values(["ts_code", "end_date", "fetched_at"], inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized[columns]


def normalize_index_member_frame(
    frame: pd.DataFrame,
    *,
    index_code: str,
    fetched_at: str | None = None,
) -> pd.DataFrame:
    columns = ["index_code", "con_code", "in_date", "out_date", "fetched_at"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    normalized = frame.copy()
    for column in ["index_code", "con_code", "in_date", "out_date"]:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized["index_code"] = normalized["index_code"].fillna(index_code).astype(str).str.strip()
    normalized["con_code"] = normalized["con_code"].fillna("").astype(str).str.strip()
    normalized["in_date"] = normalized["in_date"].map(to_yyyymmdd)
    normalized["out_date"] = normalized["out_date"].map(to_yyyymmdd)
    normalized = normalized[normalized["con_code"].ne("")].copy()
    normalized = normalize_fetched_at_column(normalized, fetched_at)
    normalized.sort_values(["con_code", "in_date", "out_date", "fetched_at"], inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized[columns]


def normalize_index_classify_frame(frame: pd.DataFrame, fetched_at: str | None = None) -> pd.DataFrame:
    columns = ["index_code", "industry_name", "level", "industry_code", "parent_code", "is_pub", "src", "fetched_at"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    normalized = frame.copy()
    for column in ["index_code", "industry_name", "level", "industry_code", "parent_code", "is_pub", "src"]:
        if column not in normalized.columns:
            normalized[column] = pd.NA
        normalized[column] = normalized[column].fillna("").astype(str).str.strip()
    normalized = normalized[normalized["index_code"].ne("")].copy()
    normalized = normalize_fetched_at_column(normalized, fetched_at)
    normalized.sort_values(["index_code", "fetched_at"], inplace=True)
    normalized.drop_duplicates(subset=["index_code"], keep="last", inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized[columns]


def normalize_broker_recommend_frame(
    frame: pd.DataFrame,
    *,
    month: str,
    fetched_at: str | None = None,
) -> pd.DataFrame:
    columns = ["month", "broker", "ts_code", "name", "fetched_at"]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    normalized = frame.copy()
    for column in ["month", "broker", "ts_code", "name"]:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    normalized["month"] = (
        normalized["month"]
        .fillna(month)
        .astype(str)
        .map(lambda value: re.sub(r"\D", "", value))
    )
    normalized["broker"] = normalized["broker"].fillna("").astype(str).str.strip()
    normalized["ts_code"] = normalized["ts_code"].fillna("").astype(str).str.strip()
    normalized["name"] = normalized["name"].fillna("").astype(str).str.strip()
    normalized = normalized[
        normalized["month"].str.len().eq(6) & normalized["broker"].ne("") & normalized["ts_code"].ne("")
    ].copy()
    normalized.loc[normalized["name"].eq(""), "name"] = normalized["ts_code"]
    normalized = normalize_fetched_at_column(normalized, fetched_at)
    normalized.sort_values(["month", "broker", "ts_code", "fetched_at"], inplace=True)
    normalized.drop_duplicates(subset=["month", "broker", "ts_code"], keep="last", inplace=True)
    normalized.reset_index(drop=True, inplace=True)
    return normalized[columns]


def create_tushare_client(token: str) -> Any:
    return ts.pro_api(token, timeout=TUSHARE_TIMEOUT_SECONDS)


def build_http_cache_key(url: str, *, method: str = "GET", data: bytes | None = None) -> str:
    digest = hashlib.sha1()
    digest.update(method.upper().encode("utf-8"))
    digest.update(b"\n")
    digest.update(url.encode("utf-8"))
    digest.update(b"\n")
    if data:
        digest.update(data)
    return digest.hexdigest()


def get_http_cache_paths(cache_key: str) -> tuple[Path, Path]:
    return (
        HTTP_CACHE_DIR / f"{cache_key}.bin",
        HTTP_CACHE_DIR / f"{cache_key}.json",
    )


def build_cached_response(url: str, content: bytes, *, method: str = "GET") -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = content
    response.url = url
    response.request = requests.Request(method.upper(), url).prepare()
    return response


def load_cached_http_response(url: str, *, method: str = "GET", data: bytes | None = None) -> requests.Response | None:
    cache_key = build_http_cache_key(url, method=method, data=data)
    cache_path, _ = get_http_cache_paths(cache_key)
    if not cache_path.exists():
        return None
    return build_cached_response(url, cache_path.read_bytes(), method=method)


def save_http_cache(url: str, content: bytes, *, method: str = "GET", data: bytes | None = None) -> None:
    cache_key = build_http_cache_key(url, method=method, data=data)
    cache_path, meta_path = get_http_cache_paths(cache_key)
    ensure_dir(cache_path.parent)
    cache_path.write_bytes(content)
    write_json(
        meta_path,
        {
            "url": url,
            "method": method.upper(),
            "saved_at": now_iso(),
        },
    )


def fetch_response(url: str, *, method: str = "GET", data: bytes | None = None) -> requests.Response:
    headers = dict(HTTP_HEADERS)
    if method == "POST":
        headers.update(
            {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://www.stats.gov.cn",
                "Referer": "https://www.stats.gov.cn/",
            }
        )

    last_error: Exception | None = None
    for attempt in range(STATS_FETCH_RETRIES):
        try:
            if method == "POST":
                response = requests.post(url, data=data, headers=headers, timeout=HTTP_TIMEOUT)
            else:
                response = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            save_http_cache(url, response.content, method=method, data=data)
            return response
        except requests.RequestException as exc:
            last_error = exc
            if attempt >= STATS_FETCH_RETRIES - 1:
                break
            wait_seconds = STATS_RETRY_BACKOFF_SECONDS[min(attempt, len(STATS_RETRY_BACKOFF_SECONDS) - 1)]
            print(
                f"[stats] request failed on attempt {attempt + 1}/{STATS_FETCH_RETRIES}: "
                f"{url} -> {exc}; retrying in {wait_seconds}s."
            )
            time.sleep(wait_seconds)

    if last_error is not None:
        cached_response = load_cached_http_response(url, method=method, data=data)
        if cached_response is not None:
            print(f"[stats] request failed; reused cached response for {url}")
            return cached_response
        raise last_error
    raise RuntimeError(f"Unexpected fetch failure state for {url}")


def fetch_html(url: str) -> str:
    response = fetch_response(url)
    response.encoding = "utf-8"
    return response.text


def extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return "\n".join(line.strip() for line in soup.get_text("\n").splitlines() if line.strip())


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", value)


def round_or_none(value: Any, digits: int = 2) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return round(numeric, digits)


def to_yyyymmdd(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) == 8 and text.isdigit():
        return text
    return ""


def to_iso_date(value: Any) -> str | None:
    normalized = to_yyyymmdd(value)
    if not normalized:
        return None
    return datetime.strptime(normalized, "%Y%m%d").strftime("%Y-%m-%d")


def quarter_end_candidates(count: int) -> list[str]:
    quarter_month_days = ((3, 31), (6, 30), (9, 30), (12, 31))
    today = date.today()
    candidates: list[str] = []
    year = today.year
    while len(candidates) < count:
        for month, day in reversed(quarter_month_days):
            quarter_end = date(year, month, day)
            if quarter_end <= today:
                candidates.append(quarter_end.strftime("%Y%m%d"))
                if len(candidates) == count:
                    break
        year -= 1
    return candidates


def percentile_rank(values: list[float], current: float | None) -> float | None:
    clean_values = [value for value in values if value is not None and math.isfinite(value)]
    if current is None or not clean_values:
        return None
    rank = sum(1 for value in clean_values if value <= current) / len(clean_values) * 100
    return round(rank, 1)


def median_or_none(values: list[float]) -> float | None:
    clean_values = [value for value in values if value is not None and math.isfinite(value)]
    if not clean_values:
        return None
    return round(float(median(clean_values)), 2)


def series_to_list(series: pd.Series, digits: int = 2) -> list[float | None]:
    return [round_or_none(value, digits) for value in series.tolist()]


def normalize_close_values(values: list[Any]) -> list[float | None]:
    if not values:
        return []

    first_valid = None
    for value in values:
        rounded = round_or_none(value)
        if rounded is not None and rounded != 0:
            first_valid = rounded
            break

    if first_valid is None:
        return [None for _ in values]

    normalized: list[float | None] = []
    for value in values:
        rounded = round_or_none(value)
        if rounded is None:
            normalized.append(None)
        else:
            normalized.append(round_or_none(rounded / first_valid * 100))
    return normalized


def load_or_update_sw_daily(pro: Any, ts_code: str, lookback_days: int = 365 * 6) -> pd.DataFrame:
    requested_start = date.today() - timedelta(days=lookback_days)
    requested_end = date.today()
    cache_path = TUSHARE_CACHE_DIR / "sw_daily" / f"{ts_code}.csv"
    runtime_df = _SW_DAILY_RUNTIME_CACHE.get(ts_code)
    if runtime_df is not None:
        if runtime_df.empty:
            print(f"[cache:sw_daily] {ts_code} runtime cache hit (empty)")
            return runtime_df.copy()
        runtime_min = runtime_df["trade_date_dt"].min().date()
        runtime_max = runtime_df["trade_date_dt"].max().date()
        if requested_start >= runtime_min and requested_end <= runtime_max:
            print(f"[cache:sw_daily] {ts_code} runtime cache hit")
            return runtime_df[
                runtime_df["trade_date_dt"].between(pd.Timestamp(requested_start), pd.Timestamp(requested_end))
            ].copy()
        cached_df = runtime_df.copy()
    else:
        cached_df = normalize_sw_daily_frame(load_cached_csv(cache_path))

    segments: list[tuple[date, date]] = []
    if cached_df.empty:
        segments.append((requested_start, requested_end))
    else:
        cached_min = cached_df["trade_date_dt"].min().date()
        cached_max = cached_df["trade_date_dt"].max().date()
        if requested_start < cached_min:
            segments.append((requested_start, cached_min - timedelta(days=1)))
        recent_start = max(requested_start, cached_max - timedelta(days=TUSHARE_RECENT_BACKFILL_DAYS))
        segments.append((recent_start, requested_end))

    fetched_frames: list[pd.DataFrame] = []
    fetch_errors: list[str] = []
    for start_date, end_date in merge_date_segments(segments):
        try:
            fetched_at = now_iso()
            raw_frame = call_tushare_with_retry(
                lambda: pro.sw_daily(
                    ts_code=ts_code,
                    start_date=start_date.strftime("%Y%m%d"),
                    end_date=end_date.strftime("%Y%m%d"),
                ),
                dataset="sw_daily",
                key=f"{ts_code}:{start_date:%Y%m%d}-{end_date:%Y%m%d}",
            )
            normalized = normalize_sw_daily_frame(raw_frame, fetched_at)
            if not normalized.empty:
                fetched_frames.append(normalized)
        except Exception as exc:
            fetch_errors.append(f"{start_date:%Y%m%d}-{end_date:%Y%m%d}: {exc}")

    merged_df = merge_cached_frames(
        [cached_df, *fetched_frames],
        key_columns=["trade_date"],
        sort_columns=["trade_date_dt"],
    )
    if fetched_frames:
        save_cached_csv(cache_path, merged_df)
        if cached_df.empty:
            print(f"[cache:sw_daily] {ts_code} initialized local history cache")
        else:
            print(f"[cache:sw_daily] {ts_code} incrementally updated {len(fetched_frames)} segment(s)")
        update_cache_state(
            "sw_daily",
            ts_code,
            "success",
            rows=len(merged_df),
            cached_start=to_iso_date(merged_df["trade_date"].iloc[0]) if not merged_df.empty else None,
            cached_end=to_iso_date(merged_df["trade_date"].iloc[-1]) if not merged_df.empty else None,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat(),
        )
    elif fetch_errors:
        if merged_df.empty:
            update_cache_state(
                "sw_daily",
                ts_code,
                "error",
                message="; ".join(fetch_errors),
                requested_start=requested_start.isoformat(),
                requested_end=requested_end.isoformat(),
            )
            raise ValueError(fetch_errors[0])
        print(f"[cache:sw_daily] {ts_code} reused cached history after fetch errors")
        update_cache_state(
            "sw_daily",
            ts_code,
            "cache_fallback",
            message="; ".join(fetch_errors),
            rows=len(merged_df),
            cached_start=to_iso_date(merged_df["trade_date"].iloc[0]) if not merged_df.empty else None,
            cached_end=to_iso_date(merged_df["trade_date"].iloc[-1]) if not merged_df.empty else None,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat(),
        )
    else:
        print(f"[cache:sw_daily] {ts_code} cache hit; no remote update needed")
        update_cache_state(
            "sw_daily",
            ts_code,
            "success",
            rows=len(merged_df),
            cached_start=to_iso_date(merged_df["trade_date"].iloc[0]) if not merged_df.empty else None,
            cached_end=to_iso_date(merged_df["trade_date"].iloc[-1]) if not merged_df.empty else None,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat(),
        )

    _SW_DAILY_RUNTIME_CACHE[ts_code] = merged_df.copy()
    return merged_df[
        merged_df["trade_date_dt"].between(pd.Timestamp(requested_start), pd.Timestamp(requested_end))
    ].copy()


def load_or_update_index_daily(pro: Any, ts_code: str, lookback_days: int = 365 * 6) -> pd.DataFrame:
    requested_start = date.today() - timedelta(days=lookback_days)
    requested_end = date.today()
    cache_path = TUSHARE_CACHE_DIR / "index_daily" / f"{ts_code}.csv"
    runtime_df = _INDEX_DAILY_RUNTIME_CACHE.get(ts_code)
    if runtime_df is not None:
        if runtime_df.empty:
            print(f"[cache:index_daily] {ts_code} runtime cache hit (empty)")
            return runtime_df.copy()
        runtime_min = runtime_df["trade_date_dt"].min().date()
        runtime_max = runtime_df["trade_date_dt"].max().date()
        if requested_start >= runtime_min and requested_end <= runtime_max:
            print(f"[cache:index_daily] {ts_code} runtime cache hit")
            return runtime_df[
                runtime_df["trade_date_dt"].between(pd.Timestamp(requested_start), pd.Timestamp(requested_end))
            ].copy()
        cached_df = runtime_df.copy()
    else:
        cached_df = normalize_index_daily_frame(load_cached_csv(cache_path))

    segments: list[tuple[date, date]] = []
    if cached_df.empty:
        segments.append((requested_start, requested_end))
    else:
        cached_min = cached_df["trade_date_dt"].min().date()
        cached_max = cached_df["trade_date_dt"].max().date()
        if requested_start < cached_min:
            segments.append((requested_start, cached_min - timedelta(days=1)))
        recent_start = max(requested_start, cached_max - timedelta(days=TUSHARE_RECENT_BACKFILL_DAYS))
        segments.append((recent_start, requested_end))

    fetched_frames: list[pd.DataFrame] = []
    fetch_errors: list[str] = []
    for start_date, end_date in merge_date_segments(segments):
        try:
            fetched_at = now_iso()
            raw_frame = call_tushare_with_retry(
                lambda: pro.index_daily(
                    ts_code=ts_code,
                    start_date=start_date.strftime("%Y%m%d"),
                    end_date=end_date.strftime("%Y%m%d"),
                ),
                dataset="index_daily",
                key=f"{ts_code}:{start_date:%Y%m%d}-{end_date:%Y%m%d}",
            )
            normalized = normalize_index_daily_frame(raw_frame, fetched_at)
            if not normalized.empty:
                fetched_frames.append(normalized)
        except Exception as exc:
            fetch_errors.append(f"{start_date:%Y%m%d}-{end_date:%Y%m%d}: {exc}")

    merged_df = merge_cached_frames(
        [cached_df, *fetched_frames],
        key_columns=["trade_date"],
        sort_columns=["trade_date_dt"],
    )
    if fetched_frames:
        save_cached_csv(cache_path, merged_df)
        if cached_df.empty:
            print(f"[cache:index_daily] {ts_code} initialized local history cache")
        else:
            print(f"[cache:index_daily] {ts_code} incrementally updated {len(fetched_frames)} segment(s)")
        update_cache_state(
            "index_daily",
            ts_code,
            "success",
            rows=len(merged_df),
            cached_start=to_iso_date(merged_df["trade_date"].iloc[0]) if not merged_df.empty else None,
            cached_end=to_iso_date(merged_df["trade_date"].iloc[-1]) if not merged_df.empty else None,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat(),
        )
    elif fetch_errors:
        if merged_df.empty:
            update_cache_state(
                "index_daily",
                ts_code,
                "error",
                message="; ".join(fetch_errors),
                requested_start=requested_start.isoformat(),
                requested_end=requested_end.isoformat(),
            )
            raise ValueError(fetch_errors[0])
        print(f"[cache:index_daily] {ts_code} reused cached history after fetch errors")
        update_cache_state(
            "index_daily",
            ts_code,
            "cache_fallback",
            message="; ".join(fetch_errors),
            rows=len(merged_df),
            cached_start=to_iso_date(merged_df["trade_date"].iloc[0]) if not merged_df.empty else None,
            cached_end=to_iso_date(merged_df["trade_date"].iloc[-1]) if not merged_df.empty else None,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat(),
        )
    else:
        print(f"[cache:index_daily] {ts_code} cache hit; no remote update needed")
        update_cache_state(
            "index_daily",
            ts_code,
            "success",
            rows=len(merged_df),
            cached_start=to_iso_date(merged_df["trade_date"].iloc[0]) if not merged_df.empty else None,
            cached_end=to_iso_date(merged_df["trade_date"].iloc[-1]) if not merged_df.empty else None,
            requested_start=requested_start.isoformat(),
            requested_end=requested_end.isoformat(),
        )

    _INDEX_DAILY_RUNTIME_CACHE[ts_code] = merged_df.copy()
    return merged_df[
        merged_df["trade_date_dt"].between(pd.Timestamp(requested_start), pd.Timestamp(requested_end))
    ].copy()


def load_or_update_roe_frames(pro: Any, periods: list[str]) -> dict[str, pd.DataFrame]:
    if periods and all(period in _ROE_RUNTIME_CACHE for period in periods):
        print(f"[cache:fina_indicator_vip] runtime cache hit for {len(periods)} period(s)")
        return {period: _ROE_RUNTIME_CACHE[period].copy() for period in periods}

    frames: dict[str, pd.DataFrame] = {}
    refresh_periods = set(periods[:TUSHARE_RECENT_ROE_QUARTERS])
    errors: list[str] = []
    refreshed_count = 0
    cache_fallback_count = 0
    cache_only_count = 0

    for period in periods:
        cache_path = TUSHARE_CACHE_DIR / "fina_indicator_vip" / f"{period}.csv"
        cached_frame = _ROE_RUNTIME_CACHE.get(period)
        if cached_frame is None:
            cached_frame = normalize_roe_frame(load_cached_csv(cache_path))

        should_refresh = period in refresh_periods or cached_frame.empty
        frame_to_use = cached_frame.copy()
        if should_refresh:
            try:
                fetched_at = now_iso()
                raw_frame = call_tushare_with_retry(
                    lambda: pro.fina_indicator_vip(period=period, fields="ts_code,end_date,roe_dt,roe"),
                    dataset="fina_indicator_vip",
                    key=period,
                )
                fetched_frame = normalize_roe_frame(raw_frame, fetched_at)
                if not fetched_frame.empty or cached_frame.empty:
                    frame_to_use = merge_cached_frames(
                        [cached_frame, fetched_frame],
                        key_columns=["ts_code", "end_date"],
                        sort_columns=["end_date"],
                    )
                    save_cached_csv(cache_path, frame_to_use)
                    refreshed_count += 1
                    update_cache_state(
                        "fina_indicator_vip",
                        period,
                        "success",
                        rows=len(frame_to_use),
                        requested_period=period,
                    )
                else:
                    cache_fallback_count += 1
                    update_cache_state(
                        "fina_indicator_vip",
                        period,
                        "cache_fallback",
                        message="empty response; reused cached quarter",
                        rows=len(frame_to_use),
                        requested_period=period,
                    )
            except Exception as exc:
                if cached_frame.empty:
                    errors.append(f"{period}: {exc}")
                    frame_to_use = normalize_roe_frame(pd.DataFrame())
                    update_cache_state(
                        "fina_indicator_vip",
                        period,
                        "error",
                        message=str(exc),
                        requested_period=period,
                    )
                else:
                    frame_to_use = cached_frame.copy()
                    cache_fallback_count += 1
                    update_cache_state(
                        "fina_indicator_vip",
                        period,
                        "cache_fallback",
                        message=str(exc),
                        rows=len(frame_to_use),
                        requested_period=period,
                    )
        else:
            cache_only_count += 1
            update_cache_state(
                "fina_indicator_vip",
                period,
                "success",
                rows=len(frame_to_use),
                requested_period=period,
            )

        _ROE_RUNTIME_CACHE[period] = frame_to_use.copy()
        frames[period] = frame_to_use.copy()

    if periods and all(frames.get(period, pd.DataFrame()).empty for period in periods):
        if errors:
            raise ValueError(errors[0])
        raise ValueError("Tushare fina_indicator_vip 未返回任何季度财务数据。")

    print(
        "[cache:fina_indicator_vip] "
        f"requested {len(periods)} period(s); refreshed {refreshed_count}, "
        f"cache-only {cache_only_count}, fallback {cache_fallback_count}"
    )
    return frames


def load_or_update_index_member(pro: Any, index_code: str) -> pd.DataFrame:
    if index_code in _INDEX_MEMBER_RUNTIME_CACHE:
        print(f"[cache:index_member] {index_code} runtime cache hit")
        return _INDEX_MEMBER_RUNTIME_CACHE[index_code].copy()

    cache_path = TUSHARE_CACHE_DIR / "index_member" / f"{index_code}.csv"
    cached_frame = normalize_index_member_frame(load_cached_csv(cache_path), index_code=index_code)
    try:
        fetched_at = now_iso()
        raw_frame = call_tushare_with_retry(
            lambda: pro.index_member(index_code=index_code),
            dataset="index_member",
            key=index_code,
        )
        fetched_frame = normalize_index_member_frame(raw_frame, index_code=index_code, fetched_at=fetched_at)
        if fetched_frame.empty and not cached_frame.empty:
            frame_to_use = cached_frame
            print(f"[cache:index_member] {index_code} reused cached snapshot after empty response")
            update_cache_state(
                "index_member",
                index_code,
                "cache_fallback",
                message="empty response; reused cached snapshot",
                rows=len(frame_to_use),
            )
        else:
            frame_to_use = fetched_frame
            save_cached_csv(cache_path, frame_to_use)
            print(f"[cache:index_member] {index_code} refreshed current constituent snapshot")
            update_cache_state(
                "index_member",
                index_code,
                "success",
                rows=len(frame_to_use),
            )
    except Exception as exc:
        if cached_frame.empty:
            update_cache_state("index_member", index_code, "error", message=str(exc))
            raise
        frame_to_use = cached_frame
        print(f"[cache:index_member] {index_code} reused cached snapshot after fetch error")
        update_cache_state(
            "index_member",
            index_code,
            "cache_fallback",
            message=str(exc),
            rows=len(frame_to_use),
        )

    _INDEX_MEMBER_RUNTIME_CACHE[index_code] = frame_to_use.copy()
    return frame_to_use.copy()


def load_or_update_index_classify(pro: Any, src: str = "SW2021") -> pd.DataFrame:
    if src in _INDEX_CLASSIFY_RUNTIME_CACHE:
        print(f"[cache:index_classify] {src} runtime cache hit")
        return _INDEX_CLASSIFY_RUNTIME_CACHE[src].copy()

    cache_path = TUSHARE_CACHE_DIR / "index_classify" / f"{src}.csv"
    cached_frame = normalize_index_classify_frame(load_cached_csv(cache_path))
    try:
        fetched_at = now_iso()
        raw_frame = call_tushare_with_retry(
            lambda: pro.index_classify(src=src),
            dataset="index_classify",
            key=src,
        )
        fetched_frame = normalize_index_classify_frame(raw_frame, fetched_at=fetched_at)
        if fetched_frame.empty and cached_frame.empty:
            raise ValueError(f"Tushare index_classify({src}) 未返回行业分类数据。")
        if fetched_frame.empty:
            frame_to_use = cached_frame
            print(f"[cache:index_classify] {src} reused cached snapshot after empty response")
            update_cache_state(
                "index_classify",
                src,
                "cache_fallback",
                message="empty response; reused cached snapshot",
                rows=len(frame_to_use),
            )
        else:
            frame_to_use = fetched_frame
            save_cached_csv(cache_path, frame_to_use)
            print(f"[cache:index_classify] {src} refreshed industry classification snapshot")
            update_cache_state("index_classify", src, "success", rows=len(frame_to_use))
    except Exception as exc:
        if cached_frame.empty:
            update_cache_state("index_classify", src, "error", message=str(exc))
            raise
        frame_to_use = cached_frame
        print(f"[cache:index_classify] {src} reused cached snapshot after fetch error")
        update_cache_state(
            "index_classify",
            src,
            "cache_fallback",
            message=str(exc),
            rows=len(frame_to_use),
        )

    _INDEX_CLASSIFY_RUNTIME_CACHE[src] = frame_to_use.copy()
    return frame_to_use.copy()


def current_constituents(pro: Any, index_code: str) -> list[str]:
    member_df = load_or_update_index_member(pro, index_code)
    if member_df.empty:
        return []

    today_str = date.today().strftime("%Y%m%d")
    active_df = member_df[
        member_df["out_date"].eq("") | member_df["out_date"].ge(today_str)
    ].copy()
    if active_df.empty:
        return []

    active_df.sort_values(["con_code", "in_date"], inplace=True)
    active_df = active_df.drop_duplicates(subset=["con_code"], keep="last")
    return active_df["con_code"].tolist()


def fetch_sw_daily(pro: Any, ts_code: str, lookback_days: int = 365 * 6) -> pd.DataFrame:
    return load_or_update_sw_daily(pro, ts_code, lookback_days=lookback_days)


def fetch_index_daily(pro: Any, ts_code: str, lookback_days: int = 365 * 6) -> pd.DataFrame:
    return load_or_update_index_daily(pro, ts_code, lookback_days=lookback_days)


def fetch_roe_frames(pro: Any, periods: list[str]) -> dict[str, pd.DataFrame]:
    return load_or_update_roe_frames(pro, periods)


def build_roe_history(
    constituents: list[str],
    roe_frames: dict[str, pd.DataFrame],
    periods: list[str],
) -> tuple[list[dict[str, Any]], float | None, str | None, float | None]:
    history: list[dict[str, Any]] = []
    values: list[float] = []

    for period in periods:
        frame = roe_frames.get(period)
        if frame is None or frame.empty:
            history.append({"period": period, "value": None})
            continue

        industry_frame = frame[frame["ts_code"].isin(constituents)].copy()
        if industry_frame.empty:
            history.append({"period": period, "value": None})
            continue

        industry_frame["roe_value"] = industry_frame["roe_dt"].where(
            industry_frame["roe_dt"].notna(),
            industry_frame["roe"],
        )
        roe_value = median_or_none(industry_frame["roe_value"].tolist())
        history.append({"period": period, "value": roe_value})
        if roe_value is not None:
            values.append(roe_value)

    current_period = None
    current_value = None
    for item in history:
        if item["value"] is not None:
            current_period = item["period"]
            current_value = item["value"]
            break

    current_percentile = percentile_rank(values, current_value)
    return history, current_value, current_period, current_percentile


def build_pb_percentiles(price_df: pd.DataFrame) -> dict[str, float | None]:
    latest_pb = round_or_none(price_df["pb"].iloc[-1])
    latest_date = price_df["trade_date_dt"].iloc[-1]
    positive_pb = price_df[price_df["pb"].gt(0)].copy()

    if positive_pb.empty:
        return {"three_year": None, "five_year": None}

    pb_3y = positive_pb[positive_pb["trade_date_dt"].ge(latest_date - timedelta(days=PB_3Y_DAYS))]["pb"].tolist()
    pb_5y = positive_pb[positive_pb["trade_date_dt"].ge(latest_date - timedelta(days=PB_5Y_DAYS))]["pb"].tolist()

    return {
        "three_year": percentile_rank(pb_3y, latest_pb),
        "five_year": percentile_rank(pb_5y, latest_pb),
    }


def build_pb_scatter_metrics(price_df: pd.DataFrame) -> tuple[float | None, str | None, float | None]:
    positive_pb = price_df[price_df["pb"].gt(0)].copy()
    if positive_pb.empty:
        return None, None, None

    latest_row = positive_pb.iloc[-1]
    latest_pb = round_or_none(latest_row["pb"])
    latest_pb_date = to_iso_date(latest_row["trade_date"])
    latest_date = latest_row["trade_date_dt"]
    pb_5y = positive_pb[positive_pb["trade_date_dt"].ge(latest_date - timedelta(days=PB_5Y_DAYS))]["pb"].tolist()
    return latest_pb, latest_pb_date, percentile_rank(pb_5y, latest_pb)


def build_crowdedness(price_df: pd.DataFrame) -> dict[str, float | str | None]:
    window_df = price_df.tail(CHART_POINTS).copy()
    if window_df.empty:
        return {
            "amount_pct": None,
            "turnover_ratio_pct": None,
            "score": None,
            "label": "数据不足",
        }

    amount_values = window_df["amount"].dropna().tolist()
    latest_amount = round_or_none(window_df["amount"].iloc[-1])

    turnover_ratio_series = window_df["amount"] / window_df["float_mv"].replace({0: pd.NA})
    turnover_ratio_values = turnover_ratio_series.dropna().tolist()
    latest_turnover_ratio = round_or_none(turnover_ratio_series.iloc[-1], 4)

    amount_pct = percentile_rank(amount_values, latest_amount)
    turnover_ratio_pct = percentile_rank(turnover_ratio_values, latest_turnover_ratio)
    if amount_pct is None or turnover_ratio_pct is None:
        return {
            "amount_pct": amount_pct,
            "turnover_ratio_pct": turnover_ratio_pct,
            "score": None,
            "label": "数据不足",
        }

    score = round((amount_pct + turnover_ratio_pct) / 2, 1)
    if score >= 80:
        label = "拥挤"
    elif score >= 50:
        label = "中性"
    else:
        label = "不拥挤"

    return {
        "amount_pct": amount_pct,
        "turnover_ratio_pct": turnover_ratio_pct,
        "score": score,
        "label": label,
    }


def rolling_percentile_inclusive(series: pd.Series, window: int) -> pd.Series:
    values = series.to_numpy(dtype=float)
    out = np.full(values.shape, np.nan, dtype=float)
    if len(values) < window:
        return pd.Series(out, index=series.index)

    windows = np.lib.stride_tricks.sliding_window_view(values, window_shape=window)
    valid = ~np.isnan(windows)
    full_window = valid.all(axis=1)
    last = windows[:, -1][:, None]
    le_last = (windows <= last) & valid
    denominator = valid.sum(axis=1)
    rank = np.divide(
        le_last.sum(axis=1),
        denominator,
        out=np.full(denominator.shape, np.nan, dtype=float),
        where=denominator > 0,
    )
    out[window - 1 :] = np.where(full_window, rank, np.nan)
    return pd.Series(out, index=series.index)


def build_crowding_hit_columns(
    frame: pd.DataFrame,
    base_name: str,
    windows: tuple[int, ...],
) -> list[str]:
    hit_columns: list[str] = []
    for window in windows:
        value_column = f"{base_name}_{window}"
        percentile_column = f"{value_column}_pct"
        hit_column = f"{value_column}_hit"
        frame[percentile_column] = rolling_percentile_inclusive(frame[value_column], CROWDING_PERCENTILE_WINDOW)
        frame[hit_column] = np.where(frame[percentile_column].notna(), frame[percentile_column] >= 0.95, np.nan)
        hit_columns.append(hit_column)
    return hit_columns


def score_crowding_group(
    frame: pd.DataFrame,
    hit_columns: list[str],
    score_column: str,
    threshold: int,
) -> pd.DataFrame:
    scored = frame.copy()
    valid = scored[hit_columns].notna().all(axis=1)
    hits = scored[hit_columns].fillna(False).sum(axis=1)
    scored[score_column] = np.where(valid, hits >= threshold, np.nan)
    return scored


def build_pb_roe_alignment(
    pb_percentiles: dict[str, float | None],
    current_roe: float | None,
    current_roe_period: str | None,
    roe_pct_20q: float | None,
) -> dict[str, Any]:
    pb_pct_3y = pb_percentiles["three_year"]
    pb_pct_5y = pb_percentiles["five_year"]
    if pb_pct_5y is None or roe_pct_20q is None:
        return {
            "pb_pct_3y": pb_pct_3y,
            "pb_pct_5y": pb_pct_5y,
            "roe_pct_20q": roe_pct_20q,
            "current_roe": current_roe,
            "current_roe_period": to_iso_date(current_roe_period) if current_roe_period else None,
            "match_gap": None,
            "status": "empty",
            "label": "数据不足",
        }

    match_gap = round(pb_pct_5y - roe_pct_20q, 1)
    if match_gap > 20:
        status = "expensive_vs_roe"
        label = "估值高于业绩"
    elif match_gap < -20:
        status = "cheap_vs_roe"
        label = "业绩强于估值"
    else:
        status = "balanced"
        label = "估值与业绩基本匹配"

    return {
        "pb_pct_3y": pb_pct_3y,
        "pb_pct_5y": pb_pct_5y,
        "roe_pct_20q": roe_pct_20q,
        "current_roe": current_roe,
        "current_roe_period": to_iso_date(current_roe_period) if current_roe_period else None,
        "match_gap": match_gap,
        "status": status,
        "label": label,
    }


def build_series_payload(
    board_id: str,
    industry: dict[str, Any],
    price_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> dict[str, Any]:
    latest_trade_date = price_df["trade_date_dt"].iloc[-1]
    chart_cutoff = latest_trade_date - timedelta(days=CHART_LOOKBACK_DAYS)
    chart_df = price_df[price_df["trade_date_dt"].ge(chart_cutoff)].copy()
    chart_df["turnover_value_ratio"] = chart_df["amount"] / chart_df["float_mv"].replace({0: pd.NA})
    aligned_benchmark = benchmark_df[benchmark_df["trade_date"].isin(chart_df["trade_date"])][["trade_date", "close"]].copy()
    aligned_benchmark.rename(columns={"close": "benchmark_close"}, inplace=True)
    chart_df = chart_df.merge(aligned_benchmark, on="trade_date", how="left")

    normalized_close = normalize_close_values(chart_df["close"].tolist())
    benchmark_normalized_close = normalize_close_values(chart_df["benchmark_close"].tolist())

    relative_to_benchmark: list[float | None] = []
    for industry_value, benchmark_value in zip(normalized_close, benchmark_normalized_close):
        if industry_value is None or benchmark_value is None or benchmark_value == 0:
            relative_to_benchmark.append(None)
        else:
            relative_to_benchmark.append(round_or_none(industry_value / benchmark_value * 100))

    return {
        "board_id": board_id,
        "ts_code": industry["ts_code"],
        "label": industry["name"],
        "source": "Tushare sw_daily",
        "benchmark_code": BENCHMARK_CODE,
        "benchmark_label": BENCHMARK_LABEL,
        "latest_trade_date": to_iso_date(chart_df["trade_date"].iloc[-1]),
        "dates": [to_iso_date(value) for value in chart_df["trade_date"].tolist()],
        "close": series_to_list(chart_df["close"]),
        "normalized_close": normalized_close,
        "benchmark_close": series_to_list(chart_df["benchmark_close"]),
        "benchmark_normalized_close": benchmark_normalized_close,
        "relative_to_benchmark": relative_to_benchmark,
        "pb": series_to_list(chart_df["pb"]),
        "amount": series_to_list(chart_df["amount"]),
        "turnover_value_ratio": series_to_list(chart_df["turnover_value_ratio"], 4),
    }


def build_ready_industry_panel(
    board_id: str,
    industry: dict[str, Any],
    price_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    constituents: list[str],
    roe_frames: dict[str, pd.DataFrame],
    roe_periods: list[str],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if price_df.empty:
        return (
            {
                "ts_code": industry["ts_code"],
                "name": industry["name"],
                "market": industry["market"],
                "enabled": True,
                "status": "error",
                "status_message": "未获取到行业日频数据。",
            },
            None,
        )

    pb_percentiles = build_pb_percentiles(price_df)
    crowdedness = build_crowdedness(price_df)
    _, current_roe, current_roe_period, roe_pct_20q = build_roe_history(constituents, roe_frames, roe_periods)
    alignment = build_pb_roe_alignment(pb_percentiles, current_roe, current_roe_period, roe_pct_20q)

    latest_row = price_df.iloc[-1]
    panel = {
        "ts_code": industry["ts_code"],
        "name": industry["name"],
        "market": industry["market"],
        "enabled": True,
        "status": "ready",
        "status_message": industry["status_message"],
        "latest_trade_date": to_iso_date(latest_row["trade_date"]),
        "latest_close": round_or_none(latest_row["close"]),
        "latest_pct_change": round_or_none(latest_row["pct_change"]),
        "pb": round_or_none(latest_row["pb"]),
        "amount": round_or_none(latest_row["amount"]),
        "float_mv": round_or_none(latest_row["float_mv"]),
        "member_count": len(constituents),
        "pb_percentiles": pb_percentiles,
        "crowdedness": crowdedness,
        "pb_roe_alignment": alignment,
    }

    if len(constituents) == 0:
        panel["status"] = "partial"
        panel["status_message"] = "已生成行业行情，但未获取到当前成分股，ROE 诊断可能缺失。"
    elif alignment["status"] == "empty":
        panel["status"] = "partial"
        panel["status_message"] = "已生成行业行情，但 ROE 历史样本不足，PB-ROE 诊断暂时保留。"

    return panel, build_series_payload(board_id, industry, price_df, benchmark_df)


def build_coming_soon_panel(industry: dict[str, Any]) -> dict[str, Any]:
    return {
        "ts_code": industry["ts_code"],
        "name": industry["name"],
        "market": industry["market"],
        "enabled": False,
        "status": "coming_soon",
        "status_message": industry["status_message"],
    }


def load_existing_sector_outputs() -> tuple[dict[str, Any], dict[str, Any]]:
    panels_path = PUBLIC_DATA_DIR / "sector-market-panels.json"
    series_path = PUBLIC_DATA_DIR / "sector-market-series.json"
    if not panels_path.exists() or not series_path.exists():
        raise SystemExit("Missing TUSHARE_TOKEN and no existing sector-market JSON files to reuse.")
    return load_json(panels_path), load_json(series_path)


def build_sector_outputs(config: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        print("TUSHARE_TOKEN missing; reusing existing sector-market JSON files.")
        return load_existing_sector_outputs()

    pro = create_tushare_client(token)
    updated_at = now_iso()
    roe_periods = quarter_end_candidates(ROE_HISTORY_QUARTERS)
    roe_frames = fetch_roe_frames(pro, roe_periods)
    benchmark_df = fetch_index_daily(pro, BENCHMARK_CODE)

    boards_output: list[dict[str, Any]] = []
    series_output: list[dict[str, Any]] = []
    ready_count = 0
    partial_count = 0
    error_count = 0
    disabled_count = 0

    for board in config["boards"]:
        industry_panels: list[dict[str, Any]] = []
        for industry in board["industries"]:
            if not industry["enabled"]:
                disabled_count += 1
                industry_panels.append(build_coming_soon_panel(industry))
                continue

            price_df = fetch_sw_daily(pro, industry["ts_code"])
            constituents = current_constituents(pro, industry["ts_code"])
            panel, series_item = build_ready_industry_panel(
                board["board_id"],
                industry,
                price_df,
                benchmark_df,
                constituents,
                roe_frames,
                roe_periods,
            )
            industry_panels.append(panel)
            if series_item is not None:
                series_output.append(series_item)

            if panel["status"] == "ready":
                ready_count += 1
            elif panel["status"] == "partial":
                partial_count += 1
            else:
                error_count += 1

        boards_output.append(
            {
                "board_id": board["board_id"],
                "title": board["title"],
                "anchor": board["anchor"],
                "description": board["description"],
                "fundamentals_placeholder": board["fundamentals_placeholder"],
                "industries": industry_panels,
            }
        )

    overall_status = "ok"
    if error_count > 0 and ready_count == 0:
        overall_status = "error"
    elif error_count > 0 or partial_count > 0:
        overall_status = "partial"

    status_message = (
        f"已生成 {ready_count} 个可用行业面板，"
        f"{partial_count} 个部分可用，{error_count} 个失败，"
        f"{disabled_count} 个占位条目待后续补充。"
    )

    panels_payload = {
        "updated_at": updated_at,
        "source": "Tushare sw_daily + index_member + fina_indicator_vip",
        "stale_after_minutes": config.get("stale_after_minutes", DEFAULT_STALE_AFTER_MINUTES),
        "status": overall_status,
        "status_message": status_message,
        "boards": boards_output,
    }

    series_payload = {
        "updated_at": updated_at,
        "source": "Tushare sw_daily + index_daily(000906.SH)",
        "stale_after_minutes": config.get("stale_after_minutes", DEFAULT_STALE_AFTER_MINUTES),
        "status": overall_status,
        "status_message": status_message,
        "series": series_output,
    }

    return panels_payload, series_payload


def clean_column_label(value: Any, default: str) -> str:
    text = str(value).strip() if value is not None else ""
    if not text or text.startswith("Unnamed:"):
        return default
    return text


def quarter_label(year: int, quarter: int) -> str:
    return f"{year}Q{quarter}"


def iter_quarter_keys(start_year: int, start_quarter: int, end_year: int, end_quarter: int) -> list[tuple[int, int]]:
    keys: list[tuple[int, int]] = []
    year = start_year
    quarter = start_quarter

    while (year, quarter) <= (end_year, end_quarter):
        keys.append((year, quarter))
        if quarter == 4:
            year += 1
            quarter = 1
        else:
            quarter += 1

    return keys


def month_label(value: str) -> str:
    normalized = re.sub(r"\D", "", str(value))
    if len(normalized) != 6:
        raise ValueError(f"Invalid month value: {value}")
    return f"{normalized[:4]}-{normalized[4:]}"


def iter_months(start_month: str, end_month: str) -> list[str]:
    start = re.sub(r"\D", "", start_month)
    end = re.sub(r"\D", "", end_month)
    if len(start) != 6 or len(end) != 6:
        raise ValueError("Month values must be in YYYYMM format.")

    year = int(start[:4])
    month = int(start[4:])
    end_year = int(end[:4])
    end_month_value = int(end[4:])
    values: list[str] = []

    while (year, month) <= (end_year, end_month_value):
        values.append(f"{year:04d}{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1

    return values


def build_macro_chart(
    chart_id: str,
    title: str,
    description: str,
    labels: list[str],
    series: list[dict[str, Any]],
    x_axis_label: str,
    latest_time_text: str,
    chart_type: str = "line",
    show_point_markers: bool = False,
    latest_changes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "chart_id": chart_id,
        "title": title,
        "description": description,
        "chart_type": chart_type,
        "x_axis_label": x_axis_label,
        "latest_label": "数据截至",
        "latest_time_text": latest_time_text,
        "show_point_markers": show_point_markers,
        "latest_changes": latest_changes or [],
        "labels": labels,
        "series": series,
    }


def build_change_item(label: str, basis_label: str, delta_value: float | None, unit: str) -> dict[str, Any] | None:
    if delta_value is None:
        return None

    rounded = round(float(delta_value), 2)
    if rounded > 0:
        direction = "up"
    elif rounded < 0:
        direction = "down"
    else:
        direction = "flat"

    return {
        "label": label,
        "basis_label": basis_label,
        "direction": direction,
        "delta_value": rounded,
        "unit": unit,
    }


def latest_sequential_change(values: list[float | None]) -> float | None:
    latest_index = None
    for index in range(len(values) - 1, -1, -1):
        value = values[index]
        if value is not None:
            latest_index = index
            break

    if latest_index is None or latest_index == 0:
        return None

    previous_index = None
    for index in range(latest_index - 1, -1, -1):
        value = values[index]
        if value is not None:
            previous_index = index
            break

    if previous_index is None:
        return None

    return values[latest_index] - values[previous_index]


def latest_same_position_yoy(records: list[dict[str, Any]]) -> tuple[str, float] | None:
    if not records:
        return None

    latest_record = max(records, key=lambda item: (item["year"], item["quarter"]))
    target_year = latest_record["year"] - 1
    target_quarter = latest_record["quarter"]
    prior_record = next(
        (
            item
            for item in records
            if item["year"] == target_year and item["quarter"] == target_quarter and item["value"] is not None
        ),
        None,
    )

    if prior_record is None or latest_record["value"] is None:
        return None

    return quarter_label(latest_record["year"], latest_record["quarter"]), latest_record["value"] - prior_record["value"]


def build_ready_macro_section(
    section_id: str,
    title: str,
    description: str,
    chart: dict[str, Any] | None = None,
    charts: list[dict[str, Any]] | None = None,
    message: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "section_id": section_id,
        "title": title,
        "description": description,
        "status": "ready",
    }
    if chart is not None:
        payload["chart"] = chart
    if charts:
        payload["charts"] = charts
    if message:
        payload["placeholder_message"] = message
    return payload


def build_error_macro_section(
    section_id: str,
    title: str,
    description: str,
    message: str,
) -> dict[str, Any]:
    return {
        "section_id": section_id,
        "title": title,
        "description": description,
        "status": "error",
        "placeholder_message": message,
    }


def build_placeholder_macro_section(
    section_id: str,
    title: str,
    description: str,
    message: str,
) -> dict[str, Any]:
    return {
        "section_id": section_id,
        "title": title,
        "description": description,
        "status": "coming_soon",
        "placeholder_message": message,
    }


def build_ready_market_section(
    section_id: str,
    title: str,
    description: str,
    chart: dict[str, Any] | None = None,
    charts: list[dict[str, Any]] | None = None,
    notice_message: str | None = None,
    coverage: dict[str, Any] | None = None,
    recommended_stocks: list[dict[str, Any]] | None = None,
    crowdedness_groups: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "section_id": section_id,
        "title": title,
        "description": description,
        "status": "ready",
    }
    if chart is not None:
        payload["chart"] = chart
    if charts is not None:
        payload["charts"] = charts
    if notice_message:
        payload["notice_message"] = notice_message
    if coverage is not None:
        payload["coverage"] = coverage
    if recommended_stocks is not None:
        payload["recommended_stocks"] = recommended_stocks
    if crowdedness_groups is not None:
        payload["crowdedness_groups"] = crowdedness_groups
    return payload


def build_error_market_section(
    section_id: str,
    title: str,
    description: str,
    message: str,
) -> dict[str, Any]:
    return {
        "section_id": section_id,
        "title": title,
        "description": description,
        "status": "error",
        "placeholder_message": message,
    }


def build_placeholder_market_section(
    section_id: str,
    title: str,
    description: str,
    message: str,
) -> dict[str, Any]:
    return {
        "section_id": section_id,
        "title": title,
        "description": description,
        "status": "coming_soon",
        "placeholder_message": message,
    }


def load_inflation_chart() -> dict[str, Any]:
    workbook_path = INPUT_DATA_DIR / "inflation.xlsx"
    frame = pd.read_excel(
        workbook_path,
        sheet_name="Sheet1",
        usecols="M:P",
        header=5,
        engine="openpyxl",
        na_values=["#N/A", "#N/A N/A"],
    )
    frame = frame.iloc[:, :4].copy()
    display_names = [
        clean_column_label(frame.columns[1], "PPI同比"),
        clean_column_label(frame.columns[2], "CPI同比"),
        clean_column_label(frame.columns[3], "核心CPI同比（剔除其他用品与服务）"),
    ]
    frame.columns = ["time", "series_1", "series_2", "series_3"]
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce", format="mixed")
    for column in ["series_1", "series_2", "series_3"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame[frame["time"].notna()].copy()
    frame = frame[frame[["series_1", "series_2", "series_3"]].notna().any(axis=1)].copy()
    frame.sort_values("time", inplace=True)
    frame.drop_duplicates(subset=["time"], keep="last", inplace=True)

    if frame.empty:
        raise ValueError("inflation.xlsx 的 M:P 列未读取到有效数据。")

    labels = frame["time"].dt.strftime("%Y-%m").tolist()
    latest_time_text = labels[-1]
    ppi_values = series_to_list(frame["series_1"])
    cpi_values = series_to_list(frame["series_2"])
    core_cpi_values = series_to_list(frame["series_3"])
    latest_changes = [
        item
        for item in [
            build_change_item(display_names[0], "环比", latest_sequential_change(ppi_values), "%"),
            build_change_item(display_names[1], "环比", latest_sequential_change(cpi_values), "%"),
            build_change_item(display_names[2], "环比", latest_sequential_change(core_cpi_values), "%"),
        ]
        if item is not None
    ]

    return build_macro_chart(
        chart_id="inflation",
        title="通胀",
        description="展示 PPI、CPI 与核心 CPI 同比走势。",
        labels=labels,
        x_axis_label="时间",
        latest_time_text=latest_time_text,
        latest_changes=latest_changes,
        series=[
            {
                "series_id": "ppi-yoy",
                "label": display_names[0],
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": ppi_values,
            },
            {
                "series_id": "cpi-yoy",
                "label": display_names[1],
                "unit": "%",
                "y_axis_id": "right",
                "span_gaps": True,
                "values": cpi_values,
            },
            {
                "series_id": "core-cpi-yoy",
                "label": display_names[2],
                "unit": "%",
                "y_axis_id": "right",
                "span_gaps": True,
                "values": core_cpi_values,
            },
        ],
    )


def load_income_chart() -> dict[str, Any]:
    workbook_path = INPUT_DATA_DIR / "employee_history.xlsx"
    frame = pd.read_excel(
        workbook_path,
        sheet_name="salary",
        usecols="X,AB",
        header=0,
        engine="openpyxl",
        na_values=["#N/A", "#N/A N/A"],
    )
    frame = frame.iloc[:, :2].copy()
    frame.columns = ["year", "salary_yoy_growth"]
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["salary_yoy_growth"] = pd.to_numeric(frame["salary_yoy_growth"], errors="coerce") * 100
    frame = frame[frame["year"].notna()].copy()
    frame = frame[frame["year"] >= INCOME_CHART_START_YEAR].copy()
    frame.sort_values("year", inplace=True)
    frame.drop_duplicates(subset=["year"], keep="last", inplace=True)

    if frame["salary_yoy_growth"].notna().sum() == 0:
        raise ValueError("employee_history.xlsx 的 salary!AB 列未读取到有效增速数据。")

    salary_by_year = {
        int(year): value
        for year, value in zip(frame["year"].tolist(), series_to_list(frame["salary_yoy_growth"]), strict=True)
    }

    income_entries = fetch_income_release_entries(start_year=INCOME_CHART_START_YEAR)
    urban_income_growth_by_quarter = {
        (entry["year"], entry["quarter"]): extract_urban_income_growth(entry["url"])
        for entry in income_entries
    }

    if not urban_income_growth_by_quarter:
        raise ValueError("未获取到城镇居民人均可支配收入增速季度数据。")

    latest_salary_year = max(salary_by_year)
    latest_urban_quarter = max(urban_income_growth_by_quarter)
    latest_year, latest_quarter = max((latest_salary_year, 4), latest_urban_quarter)

    quarter_keys = iter_quarter_keys(INCOME_CHART_START_YEAR, 1, latest_year, latest_quarter)
    labels = [quarter_label(year, quarter) for year, quarter in quarter_keys]
    salary_values = [salary_by_year.get(year) if quarter == 4 else None for year, quarter in quarter_keys]
    urban_income_values = [urban_income_growth_by_quarter.get((year, quarter)) for year, quarter in quarter_keys]

    latest_time_text = quarter_label(latest_year, latest_quarter)
    latest_changes = [
        item
        for item in [
            build_change_item("上市公司人均薪酬增速", "较上年", latest_sequential_change(salary_values), "%"),
            build_change_item("城镇居民人均可支配收入增速", "较上季", latest_sequential_change(urban_income_values), "%"),
        ]
        if item is not None
    ]

    return build_macro_chart(
        chart_id="income",
        title="收入",
        description="展示上市公司人均薪酬年度增速与国家统计局公布的城镇居民人均可支配收入季度增速。",
        labels=labels,
        x_axis_label="季度",
        latest_time_text=latest_time_text,
        latest_changes=latest_changes,
        series=[
            {
                "series_id": "listed-company-salary-yoy",
                "label": "上市公司人均薪酬增速",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": True,
                "show_point_markers": True,
                "values": salary_values,
            },
            {
                "series_id": "urban-disposable-income-yoy",
                "label": "城镇居民人均可支配收入增速",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "show_point_markers": True,
                "values": urban_income_values,
            }
        ],
    )


def coerce_quarter_label(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    match = re.search(r"(20\d{2})\D*([1-4])", text)
    if match is None:
        return None

    return quarter_label(int(match.group(1)), int(match.group(2)))


def quarter_sort_key(label: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", label)
    if match is None:
        raise ValueError(f"Invalid quarter label: {label}")
    return int(match.group(1)), int(match.group(2))


def load_household_balance_sheet_chart() -> dict[str, Any]:
    try:
        raw_frame = pd.read_excel(
            HOUSEHOLD_ASSET_PATH,
            sheet_name="估值效应_季度",
            header=1,
            engine="openpyxl",
        )
    except ValueError:
        raw_frame = pd.read_excel(
            HOUSEHOLD_ASSET_PATH,
            sheet_name=2,
            header=1,
            engine="openpyxl",
        )

    frame = raw_frame.iloc[:, 8:12].copy()
    frame.columns = ["quarter_label", "housing_qoq", "stock_qoq", "deposit_qoq"]
    frame["quarter_label"] = frame["quarter_label"].apply(coerce_quarter_label)
    frame = frame[frame["quarter_label"].notna()].copy()

    for column in ["housing_qoq", "stock_qoq", "deposit_qoq"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame.sort_values(
        "quarter_label",
        key=lambda series: series.map(quarter_sort_key),
        inplace=True,
    )
    frame.drop_duplicates(subset=["quarter_label"], keep="last", inplace=True)

    if frame.empty:
        raise ValueError("household_asset.xlsx 未读取到有效季度数据。")

    for column in ["housing_qoq", "stock_qoq", "deposit_qoq"]:
        frame[f"{column}_4q"] = frame[column].rolling(window=4, min_periods=4).sum()

    rolling_columns = ["housing_qoq_4q", "stock_qoq_4q", "deposit_qoq_4q"]
    display_frame = frame[frame[rolling_columns].notna().any(axis=1)].copy()

    if display_frame.empty:
        raise ValueError("household_asset.xlsx 未读取到可用的 4 季度滚动数据。")

    total_mask = display_frame[["housing_qoq", "stock_qoq", "deposit_qoq"]].notna().all(axis=1) & display_frame[
        rolling_columns
    ].notna().all(axis=1)
    display_frame["total_4q"] = np.where(
        total_mask,
        display_frame[rolling_columns].sum(axis=1),
        np.nan,
    )

    labels = display_frame["quarter_label"].tolist()
    latest_time_text = labels[-1]

    return build_macro_chart(
        chart_id="household-balance-sheet",
        title="居民资负表",
        description="展示居民住房资产、股票资产与储蓄存款环比变化的过去四个季度滚动累加结果，并叠加三项合计走势。",
        labels=labels,
        x_axis_label="季度",
        latest_time_text=latest_time_text,
        chart_type="combo",
        series=[
            {
                "series_id": "housing",
                "label": "住房资产环比变化（4Q累计）",
                "unit": "%",
                "y_axis_id": "left",
                "render_type": "bar",
                "stack": "household-balance-sheet",
                "span_gaps": False,
                "show_point_markers": False,
                "values": series_to_list(display_frame["housing_qoq_4q"]),
            },
            {
                "series_id": "stock",
                "label": "股票资产环比变化（4Q累计）",
                "unit": "%",
                "y_axis_id": "left",
                "render_type": "bar",
                "stack": "household-balance-sheet",
                "span_gaps": False,
                "show_point_markers": False,
                "values": series_to_list(display_frame["stock_qoq_4q"]),
            },
            {
                "series_id": "deposit",
                "label": "储蓄存款环比变化（4Q累计）",
                "unit": "%",
                "y_axis_id": "left",
                "render_type": "bar",
                "stack": "household-balance-sheet",
                "span_gaps": False,
                "show_point_markers": False,
                "values": series_to_list(display_frame["deposit_qoq_4q"]),
            },
            {
                "series_id": "total",
                "label": "三项合计（4Q累计）",
                "unit": "%",
                "y_axis_id": "left",
                "render_type": "line",
                "span_gaps": False,
                "show_point_markers": True,
                "values": series_to_list(display_frame["total_4q"]),
            },
        ],
    )


def fetch_pbc_depositor_entries(start_year: int = HOUSEHOLD_CONSUMPTION_START_YEAR) -> list[dict[str, Any]]:
    first_page_html = fetch_html(PBC_SURVEY_LIST_URL)
    total_pages_match = re.search(r'totalpage="(\d+)"', first_page_html)
    total_pages = int(total_pages_match.group(1)) if total_pages_match else 1
    entries: dict[tuple[int, int], dict[str, Any]] = {}

    for page_number in range(1, total_pages + 1):
        page_url = PBC_SURVEY_LIST_URL if page_number == 1 else urljoin(PBC_SURVEY_BASE_URL, f"11874-{page_number}.html")
        html = first_page_html if page_number == 1 else fetch_html(page_url)
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            title = " ".join(anchor.get_text(" ", strip=True).split())
            if "城镇储户问卷调查报告" not in title or not title[:4].isdigit():
                continue

            match = re.search(r"(20\d{2})年第?([一二三四1-4])季度", title)
            if match is None:
                continue

            year = int(match.group(1))
            if year < start_year:
                continue

            quarter = {"一": 1, "二": 2, "三": 3, "四": 4, "1": 1, "2": 2, "3": 3, "4": 4}[match.group(2)]
            entries[(year, quarter)] = {
                "year": year,
                "quarter": quarter,
                "title": title,
                "url": urljoin(page_url, anchor["href"]),
            }

    return [entries[key] for key in sorted(entries)]


def extract_first_pdf_url(article_url: str) -> str:
    html = fetch_html(article_url)
    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        text = " ".join(anchor.get_text(" ", strip=True).split())
        if href.lower().endswith(".pdf") or ".pdf" in text.lower():
            return urljoin(article_url, href)
    raise ValueError(f"未在页面中找到 PDF 附件：{article_url}")


def extract_pdf_text(pdf_url: str) -> str:
    response = fetch_response(pdf_url)
    reader = PdfReader(io.BytesIO(response.content))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def load_depositor_intention_chart() -> dict[str, Any]:
    patterns = {
        "more-consumption": re.compile(r'倾向于[“"]?更多消费[”"]?的居民占([0-9]+(?:\.[0-9]+)?)%'),
        "more-investment": re.compile(r'倾向于[“"]?更多投资[”"]?的居民占([0-9]+(?:\.[0-9]+)?)%'),
        "more-savings": re.compile(r'倾向于[“"]?更多储蓄[”"]?的居民占([0-9]+(?:\.[0-9]+)?)%'),
    }
    labels: list[str] = []
    values_by_series = {
        "more-consumption": [],
        "more-investment": [],
        "more-savings": [],
    }

    for entry in fetch_pbc_depositor_entries():
        pdf_url = extract_first_pdf_url(entry["url"])
        normalized_text = normalize_text(extract_pdf_text(pdf_url))
        labels.append(quarter_label(entry["year"], entry["quarter"]))

        for series_id, pattern in patterns.items():
            match = pattern.search(normalized_text)
            if match is None:
                raise ValueError(f"{entry['title']} 未提取到 {series_id} 数值")
            values_by_series[series_id].append(round(float(match.group(1)), 2))

    if not labels:
        raise ValueError("未获取到城镇储户问卷调查数据。")

    latest_changes = [
        item
        for item in [
            build_change_item("更多消费", "环比", latest_sequential_change(values_by_series["more-consumption"]), "%"),
            build_change_item("更多投资", "环比", latest_sequential_change(values_by_series["more-investment"]), "%"),
            build_change_item("更多储蓄", "环比", latest_sequential_change(values_by_series["more-savings"]), "%"),
        ]
        if item is not None
    ]

    return build_macro_chart(
        chart_id="depositor-intentions",
        title="城镇储户消费、投资与储蓄意愿",
        description="基于中国人民银行城镇储户问卷调查，展示倾向于更多消费、更多投资和更多储蓄的居民占比，覆盖 2019 年至今。",
        labels=labels,
        x_axis_label="时间",
        latest_time_text=labels[-1],
        latest_changes=latest_changes,
        series=[
            {
                "series_id": "more-consumption",
                "label": "倾向于更多消费",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": values_by_series["more-consumption"],
            },
            {
                "series_id": "more-investment",
                "label": "倾向于更多投资",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": values_by_series["more-investment"],
            },
            {
                "series_id": "more-savings",
                "label": "倾向于更多储蓄",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": values_by_series["more-savings"],
            },
        ],
    )


def search_stats_documents(query: str, page: int, page_size: int = 20) -> dict[str, Any]:
    body = (
        f"qt={quote(query)}&keyPlace=0&sort=relevance&fileType=&timeOption=0&"
        f"siteCode={STATS_SITE_CODE}&tab=&page={page}&pageSize={page_size}&ie={uuid.uuid4()}"
    )
    response = fetch_response(STATS_SEARCH_URL, method="POST", data=body.encode("utf-8"))
    return response.json()


def search_stats_exact_title(title: str) -> str | None:
    payload = search_stats_documents(title, page=1, page_size=20)
    for doc in payload.get("resultDocs", []):
        data = doc.get("data", {})
        result_title = str(data.get("titleO") or data.get("title") or "").strip()
        url = str(data.get("url") or "").replace("http://www.stats.gov.cn", "https://www.stats.gov.cn")
        if result_title == title and "stats.gov.cn" in url:
            return url
    return None


def real_estate_sales_title_candidates(year: int, month: int) -> list[str]:
    if month == 12:
        prefixes = [
            f"{year}年",
            f"{year}年1—12月份",
            f"{year}年1-12月份",
        ]
    else:
        prefixes = [
            f"{year}年1—{month}月份",
            f"{year}年1-{month}月份",
        ]

    candidates: list[str] = []
    for prefix in prefixes:
        candidates.extend(
            [
                f"{prefix}全国房地产市场基本情况",
                f"{prefix}全国房地产开发投资和销售情况",
            ]
        )
    return candidates


def parse_real_estate_sales_release_title(title: str) -> tuple[int, int] | None:
    normalized = normalize_text(title).replace("—", "-").replace("－", "-")
    if (
        "全国房地产市场基本情况" not in normalized
        and "全国房地产开发投资和销售情况" not in normalized
    ):
        return None

    year_match = re.match(r"(20\d{2})年", normalized)
    if year_match is None:
        return None
    year = int(year_match.group(1))

    range_match = re.search(r"年(\d{1,2})-(\d{1,2})月份", normalized)
    if range_match is not None:
        return year, int(range_match.group(2))

    single_match = re.search(r"年(\d{1,2})月份", normalized)
    if single_match is not None:
        return year, int(single_match.group(1))

    return year, 12


def fetch_new_home_sales_release_entries(
    start_year: int = REAL_ESTATE_HIGH_FREQUENCY_START_YEAR,
) -> list[dict[str, Any]]:
    entries: dict[tuple[int, int], dict[str, Any]] = {}

    for query in ("全国房地产市场基本情况", "全国房地产开发投资和销售情况"):
        for page in range(1, 8):
            payload = search_stats_documents(query, page=page, page_size=20)
            documents = payload.get("resultDocs", [])
            if not documents:
                break

            for doc in documents:
                data = doc.get("data", {})
                title = str(data.get("titleO") or data.get("title") or "").strip()
                url = str(data.get("url") or "").replace("http://www.stats.gov.cn", "https://www.stats.gov.cn")
                parsed = parse_real_estate_sales_release_title(title)
                if parsed is None or "stats.gov.cn" not in url:
                    continue

                year, month = parsed
                if year < start_year:
                    continue

                entries[(year, month)] = {
                    "year": year,
                    "month": month,
                    "title": title,
                    "url": url,
                }

    return [entries[key] for key in sorted(entries)]


def parse_real_estate_sales_release_title_v2(title: str) -> tuple[int, int] | None:
    normalized = normalize_text(title).replace("—", "-").replace("－", "-")
    if (
        "全国房地产市场基本情况" not in normalized
        and "全国房地产开发投资和销售情况" not in normalized
    ):
        return None

    year_match = re.match(r"(20\d{2})年", normalized)
    if year_match is None:
        return None

    if any(token in normalized for token in ("上半年", "一季度", "二季度", "三季度", "前三季度")):
        return None

    year = int(year_match.group(1))
    range_match = re.search(r"年1-(\d{1,2})月份", normalized)
    if range_match is not None:
        return year, int(range_match.group(1))

    single_match = re.search(r"年(\d{1,2})月份", normalized)
    if single_match is not None:
        return year, int(single_match.group(1))

    annual_titles = {
        f"{year}年全国房地产市场基本情况",
        f"{year}年全国房地产开发投资和销售情况",
    }
    if normalized in annual_titles:
        return year, 12

    return None


def fetch_new_home_sales_release_entries_v2(
    start_year: int = REAL_ESTATE_HIGH_FREQUENCY_START_YEAR,
    end_year: int | None = None,
) -> list[dict[str, Any]]:
    if end_year is None:
        end_year = date.today().year

    entries: dict[tuple[int, int], dict[str, Any]] = {}

    for query in ("全国房地产市场基本情况", "全国房地产开发投资和销售情况"):
        for page in range(1, 8):
            payload = search_stats_documents(query, page=page, page_size=20)
            documents = payload.get("resultDocs", [])
            if not documents:
                break

            for doc in documents:
                data = doc.get("data", {})
                title = str(data.get("titleO") or data.get("title") or "").strip()
                url = str(data.get("url") or "").replace("http://www.stats.gov.cn", "https://www.stats.gov.cn")
                parsed = parse_real_estate_sales_release_title_v2(title)
                if parsed is None or "stats.gov.cn" not in url:
                    continue

                year, month = parsed
                if year < start_year or year > end_year:
                    continue

                entries[(year, month)] = {
                    "year": year,
                    "month": month,
                    "title": title,
                    "url": url,
                }

    fallback_start_year = min(end_year, start_year + 1)
    for year in range(fallback_start_year, end_year + 1):
        for month in range(2, 13):
            if (year, month) in entries:
                continue

            for candidate in real_estate_sales_title_candidates(year, month):
                url = search_stats_exact_title(candidate)
                if url:
                    entries[(year, month)] = {
                        "year": year,
                        "month": month,
                        "title": candidate,
                        "url": url,
                    }
                    break

    return [entries[key] for key in sorted(entries)]


def extract_new_home_sales_amount(url: str) -> float:
    normalized_text = normalize_text(extract_text_from_html(fetch_html(url)))
    patterns = [
        r"新建商品房销售额([0-9]+(?:\.[0-9]+)?)亿元",
        r"商品房销售额([0-9]+(?:\.[0-9]+)?)亿元",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized_text)
        if match is not None:
            return round(float(match.group(1)), 2)
    raise ValueError(f"未从国家统计局页面提取到新建商品房销售额：{url}")


def load_new_home_sales_chart() -> dict[str, Any]:
    start_year = date.today().year - REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS
    entries = fetch_new_home_sales_release_entries(start_year=start_year)
    records: list[dict[str, Any]] = []
    for entry in entries:
        try:
            cumulative_value = extract_new_home_sales_amount(entry["url"])
        except Exception:
            continue
        records.append(
            {
                "year": entry["year"],
                "month": entry["month"],
                "label": f"{entry['year']}-{entry['month']:02d}",
                "date": pd.Timestamp(year=entry["year"], month=entry["month"], day=1),
                "cumulative_value": cumulative_value,
            }
        )

    if not records:
        raise ValueError("未抓取到新建商品房销售额数据。")

    records.sort(key=lambda item: (item["year"], item["month"]))
    prior_cumulative_by_year: dict[int, float] = {}
    records_with_values: list[dict[str, Any]] = []

    for record in records:
        prior_cumulative = prior_cumulative_by_year.get(record["year"])
        if prior_cumulative is None:
            monthly_value = None
        else:
            monthly_value = round(record["cumulative_value"] - prior_cumulative, 2)
        records_with_values.append(
            {
                "label": record["label"],
                "date": record["date"],
                "value": monthly_value,
            }
        )
        prior_cumulative_by_year[record["year"]] = record["cumulative_value"]

    latest_date = records_with_values[-1]["date"]
    window_start = latest_date - pd.DateOffset(years=REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS)
    filtered_records = [record for record in records_with_values if record["date"] >= window_start]
    if not filtered_records:
        raise ValueError("未提取到滚动三年窗口内的新建商品房销售额数据。")
    labels = [record["label"] for record in filtered_records]
    values = [record["value"] for record in filtered_records]

    return build_macro_chart(
        chart_id="new-home-sales-monthly",
        title="新建商品房销售额",
        description="来自国家统计局月度房地产市场发布页。图中以年内累计销售额做差近似单月值，1-2 月因仅公布累计值而不单独拆分。",
        labels=labels,
        x_axis_label="月份",
        latest_time_text=labels[-1],
        series=[
            {
                "series_id": "new-home-sales-monthly",
                "label": "新建商品房销售额单月值",
                "unit": "亿元",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": values,
            }
        ],
    )


SEVENTY_CITY_TITLE_PATTERN = re.compile(r"(20\d{2})年(\d{1,2})月份70个大中城市商品住宅销售价格变动情况")


def parse_seventy_city_release_title(title: str) -> tuple[int, int] | None:
    match = SEVENTY_CITY_TITLE_PATTERN.search(title)
    if match is None:
        return None
    return int(match.group(1)), int(match.group(2))


def fetch_new_home_price_release_entries(
    start_year: int = REAL_ESTATE_HIGH_FREQUENCY_START_YEAR,
) -> list[dict[str, Any]]:
    entries: dict[tuple[int, int], dict[str, Any]] = {}

    for page in range(1, 8):
        payload = search_stats_documents("70个大中城市商品住宅销售价格变动情况", page=page, page_size=20)
        documents = payload.get("resultDocs", [])
        if not documents:
            break

        for doc in documents:
            data = doc.get("data", {})
            title = str(data.get("titleO") or "").strip()
            url = str(data.get("url") or "").replace("http://www.stats.gov.cn", "https://www.stats.gov.cn")
            parsed = parse_seventy_city_release_title(title)
            if parsed is None or "stats.gov.cn" not in url:
                continue

            year, month = parsed
            if year < start_year:
                continue

            entries[(year, month)] = {
                "year": year,
                "month": month,
                "title": title,
                "url": url,
            }

    return [entries[key] for key in sorted(entries)]


def extract_new_home_price_tier_values(url: str) -> dict[str, float]:
    soup = BeautifulSoup(fetch_html(url), "html.parser")
    target_table = None
    for table in soup.find_all("table"):
        table_text = normalize_text(table.get_text(" ", strip=True))
        if "70个大中城市新建商品住宅销售价格指数" in table_text:
            target_table = table
            break

    if target_table is None:
        raise ValueError(f"未在页面中找到 70 城新建商品住宅价格表：{url}")

    city_values: dict[str, float] = {}
    for row in target_table.find_all("tr"):
        cells = [cell.get_text(" ", strip=True).replace("\xa0", "").strip() for cell in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        for index, cell in enumerate(cells[:-1]):
            city_name = cell.replace("市", "").strip()
            if city_name not in CITY_TIER_MAP:
                continue
            value = round_or_none(cells[index + 1], 2)
            if value is not None:
                city_values[city_name] = round(value - 100, 2)

    if not city_values:
        raise ValueError(f"未在页面中提取到 70 城新建商品住宅价格指数：{url}")

    tier_values: dict[str, float] = {}
    for tier in ("一线", "二线", "三线"):
        tier_series = [value for city, value in city_values.items() if CITY_TIER_MAP.get(city) == tier]
        if tier_series:
            tier_values[tier] = round(float(np.mean(tier_series)), 2)

    if len(tier_values) != 3:
        raise ValueError(f"70 城新房价格分层结果不完整：{url}")

    return tier_values


def load_new_home_price_tier_chart() -> dict[str, Any]:
    entries = fetch_new_home_price_release_entries()
    if not entries:
        raise ValueError("未抓取到 70 大中城市新建商品住宅价格发布页。")

    labels: list[str] = []
    tier_records = {"一线": [], "二线": [], "三线": []}

    for entry in entries:
        try:
            tier_values = extract_new_home_price_tier_values(entry["url"])
        except Exception:
            continue
        labels.append(f"{entry['year']}-{entry['month']:02d}")
        for tier in ("一线", "二线", "三线"):
            tier_records[tier].append(tier_values.get(tier))

    if not labels:
        raise ValueError("未提取到可用的 70 城新房价格分层数据。")

    return build_macro_chart(
        chart_id="new-home-price-tier",
        title="70大中城市新房价格",
        description="来自国家统计局 70 个大中城市商品住宅销售价格发布页，按一线、二线和三线城市对新建商品住宅价格环比变化做简单平均。",
        labels=labels,
        x_axis_label="月份",
        latest_time_text=labels[-1],
        series=[
            {
                "series_id": "new-home-price-tier1",
                "label": "一线",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": tier_records["一线"],
            },
            {
                "series_id": "new-home-price-tier2",
                "label": "二线",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": tier_records["二线"],
            },
            {
                "series_id": "new-home-price-tier3",
                "label": "三线",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": tier_records["三线"],
            },
        ],
    )


def extract_new_home_price_tier_values_v2(url: str) -> dict[str, float]:
    tables = pd.read_html(io.StringIO(fetch_html(url)))
    if len(tables) < 2:
        raise ValueError(f"未在页面中读取到可用的 70 城新房价格表：{url}")

    city_values: dict[str, float] = {}
    for table in tables[:2]:
        frame = table.copy()
        if frame.empty:
            continue

        frame = frame.iloc[2:].reset_index(drop=True)
        for offset in (0, 4):
            if frame.shape[1] < offset + 4:
                continue

            block = frame.iloc[:, offset : offset + 4].copy()
            if block.shape[1] < 4:
                continue

            block.columns = ["city", "mom", "yoy", "avg"]
            for _, row in block.iterrows():
                city_name = re.sub(r"\s+", "", str(row["city"]).replace("\xa0", "")).replace("市", "")
                if city_name not in CITY_TIER_MAP:
                    continue

                mom_value = round_or_none(row["mom"], 2)
                if mom_value is None:
                    continue

                city_values[city_name] = round(mom_value - 100, 2)

    if not city_values:
        raise ValueError(f"未在页面中提取到 70 城新房价格指数：{url}")

    tier_values: dict[str, float] = {}
    for tier in ("一线", "二线", "三线"):
        tier_series = [value for city, value in city_values.items() if CITY_TIER_MAP.get(city) == tier]
        if tier_series:
            tier_values[tier] = round(float(np.mean(tier_series)), 2)

    if len(tier_values) != 3:
        raise ValueError(f"70 城新房价格分层结果不完整：{url}")

    return tier_values


def load_new_home_price_tier_chart_v2() -> dict[str, Any]:
    start_year = date.today().year - REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS
    entries = fetch_new_home_price_release_entries(start_year=start_year)
    if not entries:
        raise ValueError("未抓取到 70 大中城市新房价格发布页。")

    records: list[dict[str, Any]] = []
    for entry in entries:
        try:
            tier_values = extract_new_home_price_tier_values_v2(entry["url"])
        except Exception:
            continue

        records.append(
            {
                "label": f"{entry['year']}-{entry['month']:02d}",
                "date": pd.Timestamp(year=entry["year"], month=entry["month"], day=1),
                "tier_values": tier_values,
            }
        )

    if not records:
        raise ValueError("未提取到可用的 70 城新房价格分层数据。")

    records.sort(key=lambda item: item["date"])
    latest_date = records[-1]["date"]
    window_start = latest_date - pd.DateOffset(years=REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS)
    filtered_records = [record for record in records if record["date"] >= window_start]
    if not filtered_records:
        raise ValueError("未提取到滚动三年窗口内的 70 城新房价格数据。")

    labels = [record["label"] for record in filtered_records]
    tier_records = {"一线": [], "二线": [], "三线": []}
    for record in filtered_records:
        for tier in ("一线", "二线", "三线"):
            tier_records[tier].append(record["tier_values"].get(tier))

    return build_macro_chart(
        chart_id="new-home-price-tier",
        title="70大中城市新房价格",
        description="来自国家统计局 70 个大中城市商品住宅销售价格发布页，按一线、二线和三线城市对新建商品住宅价格环比变化做简单平均，并统一展示最近滚动 3 年。",
        labels=labels,
        x_axis_label="月份",
        latest_time_text=labels[-1],
        series=[
            {
                "series_id": "new-home-price-tier1",
                "label": "一线",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": tier_records["一线"],
            },
            {
                "series_id": "new-home-price-tier2",
                "label": "二线",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": tier_records["二线"],
            },
            {
                "series_id": "new-home-price-tier3",
                "label": "三线",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": tier_records["三线"],
            },
        ],
    )


def load_bingshan_index_frame() -> pd.DataFrame:
    if not BINGSHAN_INDEX_PATH.exists():
        raise FileNotFoundError(f"未找到手动更新的文件：{BINGSHAN_INDEX_PATH}")

    source_frame = pd.read_excel(
        BINGSHAN_INDEX_PATH,
        sheet_name="分能级计算",
        header=[0, 1],
        engine="openpyxl",
    )

    date_column = next(
        (
            column
            for column in source_frame.columns
            if clean_column_label(column[1], "") == "日期"
        ),
        None,
    )
    if date_column is None:
        raise ValueError("bingshan_index.xlsx 未找到日期列。")

    group_specs = ["全国", "一线4", "强二线5", "弱二线22", "三线13"]
    metric_specs = ["活跃度", "价格环比"]

    selected_columns: dict[str, pd.Series] = {
        "date": pd.to_datetime(source_frame[date_column], errors="coerce"),
    }

    for group_label in group_specs:
        for metric_label in metric_specs:
            column = find_bingshan_column(source_frame, group_label, metric_label)
            selected_columns[f"{group_label}__{metric_label}"] = pd.to_numeric(source_frame[column], errors="coerce")

    frame = pd.DataFrame(selected_columns)
    frame = frame[frame["date"].notna()].copy()
    value_columns = [column for column in frame.columns if column != "date"]
    frame = frame[frame[value_columns].notna().any(axis=1)].copy()

    frame.sort_values("date", inplace=True)
    frame.drop_duplicates(subset=["date"], keep="last", inplace=True)
    frame.reset_index(drop=True, inplace=True)
    return frame


def find_bingshan_column(frame: pd.DataFrame, group_label: str, metric_label: str) -> Any:
    for column in frame.columns:
        if not isinstance(column, tuple):
            continue
        top_label = clean_column_label(column[0], "")
        sub_label = clean_column_label(column[1], "")
        if top_label == group_label and sub_label == metric_label:
            return column
    raise KeyError(f"未找到 bingshan_index 列：{group_label} / {metric_label}")


def load_second_hand_chart(chart_id: str, title: str, description: str, metric_label: str, unit: str) -> dict[str, Any]:
    frame = load_bingshan_index_frame()
    latest_date = frame["date"].max()
    rolling_window_start = latest_date - pd.DateOffset(years=REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS)
    frame = frame[frame["date"] >= rolling_window_start].copy()
    group_specs = [
        ("全国", "全国"),
        ("一线4", "一线"),
        ("强二线5", "强二线"),
        ("弱二线22", "弱二线"),
        ("三线13", "三线"),
    ]

    series: list[dict[str, Any]] = []
    labels = frame["date"].dt.strftime("%Y-%m-%d").tolist()
    has_any_value = False

    for source_label, display_label in group_specs:
        column = f"{source_label}__{metric_label}"
        values = pd.to_numeric(frame[column], errors="coerce")
        if metric_label == "价格环比":
            values = values * 100
        if values.notna().any():
            has_any_value = True
        series.append(
            {
                "series_id": f"{chart_id}-{display_label}",
                "label": display_label,
                "unit": unit,
                "y_axis_id": "left",
                "span_gaps": False,
                "values": series_to_list(values),
            }
        )

    if not has_any_value:
        raise ValueError(f"{title} 未读取到有效序列。")

    return build_macro_chart(
        chart_id=chart_id,
        title=title,
        description=description,
        labels=labels,
        x_axis_label="周度时间",
        latest_time_text=labels[-1],
        series=series,
    )


def load_second_hand_price_mom_chart() -> dict[str, Any]:
    return load_second_hand_chart(
        chart_id="second-hand-price-mom",
        title="二手房价格月环比",
        description="冰山统计的全国、一线、强二线、弱二线和三线城市的二手房价格月环比走势。",
        metric_label="价格环比",
        unit="%",
    )


def load_second_hand_activity_chart() -> dict[str, Any]:
    return load_second_hand_chart(
        chart_id="second-hand-activity",
        title="二手房活跃度",
        description="冰山统计的全国、一线、强二线、弱二线和三线城市的二手房带看活跃度走势。",
        metric_label="活跃度",
        unit="指数",
    )


def parse_income_release_stage(title: str) -> tuple[int, int] | None:
    if "居民收入和消费支出情况" not in title:
        return None

    match = re.search(r"(20\d{2})年", title)
    if match is None:
        return None

    year = int(match.group(1))
    if "一季度" in title:
        return year, 1
    if "上半年" in title:
        return year, 2
    if "前三季度" in title:
        return year, 3
    if title.strip() == f"{year}年居民收入和消费支出情况":
        return year, 4
    return None


def fetch_income_release_entries(start_year: int = HOUSEHOLD_CONSUMPTION_START_YEAR) -> list[dict[str, Any]]:
    entries: dict[tuple[int, int], dict[str, Any]] = {}

    for page in range(1, 6):
        payload = search_stats_documents("居民收入和消费支出情况", page=page, page_size=20)
        for doc in payload.get("resultDocs", []):
            data = doc.get("data", {})
            title = str(data.get("titleO") or "")
            url = str(data.get("url") or "").replace("http://www.stats.gov.cn", "https://www.stats.gov.cn")
            if not title or "stats.gov.cn" not in url:
                continue

            parsed = parse_income_release_stage(title)
            if parsed is None:
                continue

            year, quarter = parsed
            if year < start_year:
                continue

            entries[(year, quarter)] = {
                "year": year,
                "quarter": quarter,
                "title": title,
                "url": url,
            }

    return [entries[key] for key in sorted(entries)]


def extract_urban_income_and_consumption(url: str) -> tuple[float, float]:
    normalized_text = normalize_text(extract_text_from_html(fetch_html(url)))
    income_match = re.search(r"城镇居民人均可支配收入([0-9]+(?:\.[0-9]+)?)元", normalized_text)
    consumption_match = re.search(r"城镇居民人均消费支出([0-9]+(?:\.[0-9]+)?)元", normalized_text)
    if income_match is None or consumption_match is None:
        raise ValueError(f"未从国家统计局页面提取到城镇居民收入/消费支出：{url}")
    return float(income_match.group(1)), float(consumption_match.group(1))


def extract_urban_income_growth(url: str) -> float:
    normalized_text = normalize_text(extract_text_from_html(fetch_html(url)))
    match = re.search(
        r"城镇居民人均可支配收入([0-9]+(?:\.[0-9]+)?)元，(?:比上年)?增长(?:（[^）]*）)?([0-9]+(?:\.[0-9]+)?)%",
        normalized_text,
    )
    if match is None:
        raise ValueError(f"未从国家统计局页面提取到城镇居民人均可支配收入增速：{url}")
    return round(float(match.group(2)), 2)


def load_urban_apc_chart() -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for entry in fetch_income_release_entries():
        urban_income, urban_consumption = extract_urban_income_and_consumption(entry["url"])
        records.append(
            {
                "year": entry["year"],
                "quarter": entry["quarter"],
                "value": round(urban_consumption / urban_income * 100, 2),
            }
        )

    if not records:
        raise ValueError("未获取到城镇居民平均消费倾向数据。")

    labels = ["Q1", "Q2", "Q3", "Q4"]
    latest_record = max(records, key=lambda item: (item["year"], item["quarter"]))
    series: list[dict[str, Any]] = []
    latest_yoy = latest_same_position_yoy(records)
    latest_changes: list[dict[str, Any]] = []

    if latest_yoy is not None:
        latest_change = build_change_item(latest_yoy[0], "同比", latest_yoy[1], "%")
        if latest_change is not None:
            latest_changes.append(latest_change)

    for year in sorted({record["year"] for record in records}):
        values_by_quarter = {record["quarter"]: record["value"] for record in records if record["year"] == year}
        series.append(
            {
                "series_id": f"urban-apc-{year}",
                "label": str(year),
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": [values_by_quarter.get(quarter) for quarter in range(1, 5)],
            }
        )

    return build_macro_chart(
        chart_id="urban-apc-seasonal",
        title="城镇居民平均消费倾向",
        description="根据国家统计局季度发布的城镇居民人均消费支出与城镇居民人均可支配收入计算，按季度节点展示 2019 年以来的季节性变化。",
        labels=labels,
        x_axis_label="季度",
        latest_time_text=quarter_label(latest_record["year"], latest_record["quarter"]),
        show_point_markers=True,
        latest_changes=latest_changes,
        series=series,
    )


def load_consumer_confidence_chart() -> dict[str, Any]:
    workbook_path = INPUT_DATA_DIR / "consumer_confidence.xlsx"
    frame = pd.read_excel(
        workbook_path,
        sheet_name=0,
        header=1,
        engine="openpyxl",
        na_values=["#N/A", "#N/A N/A"],
    )
    frame = frame.iloc[:, :5].copy()
    frame.columns = ["time", "confidence", "employment", "income", "consumption"]
    frame["time"] = pd.to_datetime(frame["time"], errors="coerce", format="mixed")
    for column in ["confidence", "employment", "income", "consumption"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame[frame["time"].notna()].copy()
    frame = frame[frame["time"].ge(pd.Timestamp(f"{HOUSEHOLD_CONSUMPTION_START_YEAR}-01-01"))].copy()
    frame = frame[frame[["confidence", "employment", "income", "consumption"]].notna().any(axis=1)].copy()
    frame.sort_values("time", inplace=True)
    frame.drop_duplicates(subset=["time"], keep="last", inplace=True)

    if frame.empty:
        raise ValueError("consumer_confidence.xlsx 未读取到有效的消费者信心数据。")

    labels = frame["time"].dt.strftime("%Y-%m").tolist()
    confidence_values = series_to_list(frame["confidence"])
    employment_values = series_to_list(frame["employment"])
    income_values = series_to_list(frame["income"])
    consumption_values = series_to_list(frame["consumption"])
    latest_changes = [
        item
        for item in [
            build_change_item("总指数", "环比", latest_sequential_change(confidence_values), "点"),
            build_change_item("就业", "环比", latest_sequential_change(employment_values), "点"),
            build_change_item("收入", "环比", latest_sequential_change(income_values), "点"),
            build_change_item("消费意愿", "环比", latest_sequential_change(consumption_values), "点"),
        ]
        if item is not None
    ]

    return build_macro_chart(
        chart_id="consumer-confidence",
        title="消费者信心",
        description="手动维护的国家统计局月度消费者信心数据，包含总指数以及就业、收入和消费意愿三个分项。",
        labels=labels,
        x_axis_label="时间",
        latest_time_text=labels[-1],
        latest_changes=latest_changes,
        series=[
            {
                "series_id": "consumer-confidence",
                "label": "消费者信心",
                "unit": "点",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": confidence_values,
            },
            {
                "series_id": "employment-confidence",
                "label": "就业",
                "unit": "点",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": employment_values,
            },
            {
                "series_id": "income-confidence",
                "label": "收入",
                "unit": "点",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": income_values,
            },
            {
                "series_id": "consumption-willingness",
                "label": "消费意愿",
                "unit": "点",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": consumption_values,
            },
        ],
    )


def build_household_consumption_section() -> dict[str, Any]:
    description = "跟踪央行储户消费、投资与储蓄意愿，城镇居民平均消费倾向，以及消费者信心月度指标。"
    chart_loaders = [
        ("城镇储户消费、投资与储蓄意愿", load_depositor_intention_chart),
        ("城镇居民平均消费倾向", load_urban_apc_chart),
        ("消费者信心", load_consumer_confidence_chart),
    ]
    charts: list[dict[str, Any]] = []
    errors: list[str] = []

    for label, loader in chart_loaders:
        try:
            charts.append(loader())
        except Exception as exc:
            errors.append(f"{label}：{exc}")

    if charts:
        message = None
        if errors:
            message = "部分图表未生成：" + "；".join(errors)
        return build_ready_macro_section(
            section_id="household-consumption-intention",
            title="居民消费与消费意愿",
            description=description,
            charts=charts,
            message=message,
        )

    return build_error_macro_section(
        section_id="household-consumption-intention",
        title="居民消费与消费意愿",
        description=description,
        message="居民消费与消费意愿数据暂未生成：" + "；".join(errors or ["暂无可用图表"]),
    )


def build_macro_board(updated_at: str, stale_after_minutes: int) -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    ready_count = 0
    placeholder_count = 0
    error_count = 0

    inflation_description = "直接调用 input_data/inflation.xlsx 的 M:P 列，展示 PPI、CPI 与核心CPI同比走势。"
    income_description = "展示上市公司人均薪酬增速与国家统计局公布的城镇居民人均可支配收入增速。"

    try:
        sections.append(
            build_ready_macro_section(
                section_id="inflation",
                title="通胀",
                description=inflation_description,
                chart=load_inflation_chart(),
            )
        )
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_macro_section(
                section_id="inflation",
                title="通胀",
                description=inflation_description,
                message=f"通胀图暂未生成：{exc}",
            )
        )
        error_count += 1

    try:
        sections.append(
            build_ready_macro_section(
                section_id="income",
                title="收入",
                description=income_description,
                chart=load_income_chart(),
            )
        )
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_macro_section(
                section_id="income",
                title="收入",
                description=income_description,
                message=f"收入图暂未生成：{exc}",
            )
        )
        error_count += 1

    sections.append(
        build_placeholder_macro_section(
            section_id="household-balance-sheet",
            title="居民资负表",
            description="后续将在这里补充居民资产负债表相关数据。",
            message="居民资负表数据暂未接入。",
        )
    )
    placeholder_count += 1

    sections.append(
        build_placeholder_macro_section(
            section_id="household-consumption-intention",
            title="居民消费与消费意愿",
            description="后续将在这里补充居民消费、储蓄意愿与消费意愿相关数据。",
            message="居民消费与消费意愿数据暂未接入。",
        )
    )
    placeholder_count += 1

    status = "ok"
    if error_count > 0 and ready_count == 0:
        status = "error"
    elif error_count > 0 or placeholder_count > 0:
        status = "partial"

    return {
        "updated_at": updated_at,
        "source": "input_data/inflation.xlsx + input_data/employee_history.xlsx",
        "stale_after_minutes": stale_after_minutes,
        "status": status,
        "status_message": f"已接入 {ready_count} 个宏观栏目，{placeholder_count} 个栏目保留占位，{error_count} 个栏目加载失败。",
        "board_id": "macro",
        "title": "宏观",
        "description": "宏观看板先接入通胀与收入图表，居民资负表与居民消费意愿暂保留占位。",
        "sections": sections,
    }


def build_household_consumption_section_v2() -> dict[str, Any]:
    description = "跟踪央行储户消费、投资与储蓄意愿，城镇居民平均消费倾向，以及消费者信心月度指标。"
    chart_loaders = [
        ("城镇储户消费、投资与储蓄意愿", load_depositor_intention_chart),
        ("城镇居民平均消费倾向", load_urban_apc_chart),
        ("消费者信心", load_consumer_confidence_chart),
    ]
    charts: list[dict[str, Any]] = []
    errors: list[str] = []

    for label, loader in chart_loaders:
        try:
            charts.append(loader())
        except Exception as exc:
            errors.append(f"{label}：{exc}")

    if charts:
        message = None
        if errors:
            message = "部分图表未生成：" + "；".join(errors)
        return build_ready_macro_section(
            section_id="household-consumption-intention",
            title="居民消费与消费意愿",
            description=description,
            charts=charts,
            message=message,
        )

    return build_error_macro_section(
        section_id="household-consumption-intention",
        title="居民消费与消费意愿",
        description=description,
        message="居民消费与消费意愿数据暂未生成：" + "；".join(errors or ["暂无可用图表"]),
    )


def build_macro_board_v2(updated_at: str, stale_after_minutes: int) -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    ready_count = 0
    placeholder_count = 0
    error_count = 0

    inflation_description = "展示 PPI、CPI 与核心 CPI 同比走势。"
    income_description = "展示上市公司人均薪酬增速与国家统计局公布的城镇居民人均可支配收入增速。"

    try:
        sections.append(
            build_ready_macro_section(
                section_id="inflation",
                title="通胀",
                description=inflation_description,
                chart=load_inflation_chart(),
            )
        )
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_macro_section(
                section_id="inflation",
                title="通胀",
                description=inflation_description,
                message=f"通胀图暂未生成：{exc}",
            )
        )
        error_count += 1

    try:
        sections.append(
            build_ready_macro_section(
                section_id="income",
                title="收入",
                description=income_description,
                chart=load_income_chart(),
            )
        )
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_macro_section(
                section_id="income",
                title="收入",
                description=income_description,
                message=f"收入图暂未生成：{exc}",
            )
        )
        error_count += 1

    sections.append(
        build_placeholder_macro_section(
            section_id="household-balance-sheet",
            title="居民资负表",
            description="后续将在这里补充居民资产负债表相关数据。",
            message="居民资负表数据暂未接入。",
        )
    )
    placeholder_count += 1

    consumption_section = build_household_consumption_section_v2()
    sections.append(consumption_section)
    if consumption_section["status"] == "ready":
        ready_count += 1
    elif consumption_section["status"] == "coming_soon":
        placeholder_count += 1
    else:
        error_count += 1

    status = "ok"
    if error_count > 0 and ready_count == 0:
        status = "error"
    elif error_count > 0 or placeholder_count > 0:
        status = "partial"

    return {
        "updated_at": updated_at,
        "source": "内部通胀与收入数据表 + 中国人民银行 + 国家统计局 + 手动维护的消费者信心月度数据",
        "stale_after_minutes": stale_after_minutes,
        "status": status,
        "status_message": f"已接入 {ready_count} 个宏观栏目，{placeholder_count} 个栏目保留占位，{error_count} 个栏目加载失败。",
        "board_id": "macro",
        "title": "宏观",
        "description": "宏观看板当前接入通胀、收入，以及居民消费与消费意愿三组图表；居民资负表暂保留占位。",
        "sections": sections,
    }


def build_macro_board_v2(updated_at: str, stale_after_minutes: int) -> dict[str, Any]:
    sections: list[dict[str, Any]] = []
    ready_count = 0
    placeholder_count = 0
    error_count = 0

    inflation_description = "展示 PPI、CPI 与核心 CPI 同比走势。"
    income_description = "展示上市公司人均薪酬年度增速与国家统计局公布的城镇居民人均可支配收入季度增速。"
    balance_sheet_description = "展示居民住房资产、股票资产与储蓄存款环比变化的过去四个季度滚动累加结果，并叠加三项合计走势。"

    try:
        sections.append(
            build_ready_macro_section(
                section_id="inflation",
                title="通胀",
                description=inflation_description,
                chart=load_inflation_chart(),
            )
        )
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_macro_section(
                section_id="inflation",
                title="通胀",
                description=inflation_description,
                message=f"通胀图暂未生成：{exc}",
            )
        )
        error_count += 1

    try:
        sections.append(
            build_ready_macro_section(
                section_id="income",
                title="收入",
                description=income_description,
                chart=load_income_chart(),
            )
        )
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_macro_section(
                section_id="income",
                title="收入",
                description=income_description,
                message=f"收入图暂未生成：{exc}",
            )
        )
        error_count += 1

    try:
        sections.append(
            build_ready_macro_section(
                section_id="household-balance-sheet",
                title="居民资负表",
                description=balance_sheet_description,
                chart=load_household_balance_sheet_chart(),
            )
        )
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_macro_section(
                section_id="household-balance-sheet",
                title="居民资负表",
                description=balance_sheet_description,
                message=f"居民资负表图暂未生成：{exc}",
            )
        )
        error_count += 1

    consumption_section = build_household_consumption_section_v2()
    sections.append(consumption_section)
    if consumption_section["status"] == "ready":
        ready_count += 1
    elif consumption_section["status"] == "coming_soon":
        placeholder_count += 1
    else:
        error_count += 1

    status = "ok"
    if error_count > 0 and ready_count == 0:
        status = "error"
    elif error_count > 0 or placeholder_count > 0:
        status = "partial"

    return {
        "updated_at": updated_at,
        "source": "内部通胀与收入数据表 + input_data/household_asset.xlsx + 中国人民银行 + 国家统计局 + 手动维护的消费者信心月度数据",
        "stale_after_minutes": stale_after_minutes,
        "status": status,
        "status_message": f"已接入 {ready_count} 个宏观栏目，{placeholder_count} 个栏目保留占位，{error_count} 个栏目加载失败。",
        "board_id": "macro",
        "title": "宏观",
        "description": "宏观看板当前接入通胀、收入、居民资负表，以及居民消费与消费意愿四组图表。",
        "sections": sections,
    }


def load_existing_market_tracking_output(updated_at: str, stale_after_minutes: int) -> dict[str, Any]:
    if MARKET_TRACKING_PATH.exists():
        return load_json(MARKET_TRACKING_PATH)

    sections = [
        build_placeholder_market_section(
            section_id="consumer-pb-roe",
            title="消费相关行业 PB-ROE 图",
            description="后续将在这里补充消费相关行业 PB 分位数与 ROE 分位数对比。",
            message="消费相关行业 PB-ROE 图暂未接入。",
        ),
        build_placeholder_market_section(
            section_id="consumer-crowdedness",
            title="消费相关行业拥挤度跟踪",
            description="后续将在这里补充消费相关行业拥挤度跟踪。",
            message="消费相关行业拥挤度图暂未接入。",
        ),
        build_error_market_section(
            section_id="consumer-broker-share",
            title="券商金股中消费个股占比统计",
            description="统计券商每月荐股中消费个股占全部荐股条目的比例，并展示最新月份消费个股及对应推荐券商。",
            message="缺少 TUSHARE_TOKEN，且未找到既有 market-tracking-board.json。",
        ),
    ]

    return {
        "updated_at": updated_at,
        "source": "fallback",
        "stale_after_minutes": stale_after_minutes,
        "status": "partial",
        "status_message": "市场跟踪板块使用回退数据：2 个栏目保留占位，1 个栏目加载失败。",
        "board_id": "market-tracking",
        "title": "市场跟踪",
        "description": "市场跟踪板块用于补充消费相关行业 PB-ROE、拥挤度和券商金股消费占比观察。",
        "sections": sections,
    }


def fetch_broker_recommend_history(pro: Any, start_month: str = MARKET_TRACKING_START_MONTH) -> pd.DataFrame:
    end_month = date.today().strftime("%Y%m")
    cache_key = (start_month, end_month)
    if cache_key in _BROKER_RECOMMEND_RUNTIME_CACHE:
        print(f"[cache:broker_recommend] runtime cache hit for {start_month}-{end_month}")
        return _BROKER_RECOMMEND_RUNTIME_CACHE[cache_key].copy()

    months = iter_months(start_month, end_month)
    refresh_months = set(months[-TUSHARE_RECENT_BROKER_MONTHS:])
    frames: list[pd.DataFrame] = []
    fetch_errors: list[str] = []
    refreshed_count = 0
    cache_fallback_count = 0
    cache_only_count = 0

    for month in months:
        cache_path = TUSHARE_CACHE_DIR / "broker_recommend" / f"{month}.csv"
        cached_frame = normalize_broker_recommend_frame(load_cached_csv(cache_path), month=month)
        should_refresh = month in refresh_months or cached_frame.empty
        frame_to_use = cached_frame.copy()

        if should_refresh:
            try:
                fetched_at = now_iso()
                raw_frame = call_tushare_with_retry(
                    lambda month_value=month: pro.broker_recommend(month=month_value),
                    dataset="broker_recommend",
                    key=month,
                )
                fetched_frame = normalize_broker_recommend_frame(raw_frame, month=month, fetched_at=fetched_at)
                if not fetched_frame.empty or cached_frame.empty:
                    frame_to_use = merge_cached_frames(
                        [cached_frame, fetched_frame],
                        key_columns=["month", "broker", "ts_code"],
                        sort_columns=["month", "broker", "ts_code"],
                    )
                    save_cached_csv(cache_path, frame_to_use)
                    refreshed_count += 1
                    update_cache_state("broker_recommend", month, "success", rows=len(frame_to_use))
                else:
                    cache_fallback_count += 1
                    update_cache_state(
                        "broker_recommend",
                        month,
                        "cache_fallback",
                        message="empty response; reused cached month",
                        rows=len(frame_to_use),
                    )
            except Exception as exc:
                if cached_frame.empty:
                    fetch_errors.append(f"{month}: {exc}")
                    update_cache_state("broker_recommend", month, "error", message=str(exc))
                    continue
                frame_to_use = cached_frame
                cache_fallback_count += 1
                update_cache_state(
                    "broker_recommend",
                    month,
                    "cache_fallback",
                    message=str(exc),
                    rows=len(frame_to_use),
                )
        else:
            cache_only_count += 1
            update_cache_state("broker_recommend", month, "success", rows=len(frame_to_use))

        if not frame_to_use.empty:
            frames.append(frame_to_use)

    if not frames:
        if fetch_errors:
            raise ValueError(fetch_errors[0])
        raise ValueError("Tushare broker_recommend 未返回任何月度荐股数据。")

    history = merge_cached_frames(
        frames,
        key_columns=["month", "broker", "ts_code"],
        sort_columns=["month", "broker", "ts_code"],
    )
    print(
        "[cache:broker_recommend] "
        f"requested {len(months)} month(s); refreshed {refreshed_count}, "
        f"cache-only {cache_only_count}, fallback {cache_fallback_count}"
    )
    _BROKER_RECOMMEND_RUNTIME_CACHE[cache_key] = history.copy()
    return history


def fetch_sw2021_classify(pro: Any) -> pd.DataFrame:
    classify_df = load_or_update_index_classify(pro, src="SW2021")
    if classify_df.empty:
        raise ValueError("Tushare index_classify(SW2021) 未返回行业分类数据。")

    classify_df = classify_df.copy()
    for column in ["index_code", "industry_name", "level", "industry_code", "parent_code", "is_pub", "src"]:
        if column in classify_df.columns:
            classify_df[column] = classify_df[column].fillna("").astype(str).str.strip()
    if "is_pub" in classify_df.columns:
        classify_df = classify_df[classify_df["is_pub"].eq("1")].copy()
    classify_df = classify_df[classify_df["index_code"].ne("")].copy()
    classify_df.drop_duplicates(subset=["index_code"], keep="last", inplace=True)
    classify_df.reset_index(drop=True, inplace=True)
    return classify_df


def build_consumer_stock_industry_map(pro: Any, classify_df: pd.DataFrame | None = None) -> dict[str, str]:
    if classify_df is None:
        classify_df = fetch_sw2021_classify(pro)
    l1_classify = classify_df[classify_df["level"].eq("L1")].copy()
    classify_map = {
        str(row["index_code"]).strip(): str(row["industry_name"]).strip()
        for _, row in l1_classify.iterrows()
    }

    stock_to_industry: dict[str, str] = {}
    for index_code, fallback_name in CONSUMER_MARKET_INDUSTRY_CODES.items():
        industry_name = classify_map.get(index_code, fallback_name)
        for ts_code in current_constituents(pro, index_code):
            stock_to_industry.setdefault(ts_code, industry_name)

    if not stock_to_industry:
        raise ValueError("未从 Tushare 申万一级行业成分中识别到消费个股。")

    return stock_to_industry


def build_consumer_crowding_definitions(classify_df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    l1_classify = classify_df[classify_df["level"].eq("L1")].copy()
    l2_classify = classify_df[classify_df["level"].eq("L2")].copy()
    l1_definitions: list[dict[str, Any]] = []
    l2_definitions: list[dict[str, Any]] = []

    for index_code, fallback_name in CONSUMER_MARKET_INDUSTRY_CODES.items():
        parent_matches = l1_classify[l1_classify["index_code"].eq(index_code)].copy()
        if parent_matches.empty:
            raise ValueError(f"未在 SW2021 分类中找到消费一级行业：{index_code}")
        parent = parent_matches.iloc[0]
        l1_name = str(parent["industry_name"]).strip() or fallback_name
        parent_industry_code = str(parent["industry_code"]).strip()

        l1_definitions.append(
            {
                "ts_code": index_code,
                "industry_name": l1_name,
                "level": "L1",
                "parent_l1_name": None,
            }
        )

        children = l2_classify[l2_classify["parent_code"].eq(parent_industry_code)].copy()
        children.sort_values(["industry_name", "index_code"], inplace=True)
        for _, child in children.iterrows():
            l2_definitions.append(
                {
                    "ts_code": str(child["index_code"]).strip(),
                    "industry_name": str(child["industry_name"]).strip(),
                    "level": "L2",
                    "parent_l1_name": l1_name,
                }
            )

    if not l2_definitions:
        raise ValueError("未从 SW2021 分类中展开出消费相关二级行业。")

    return l1_definitions, l2_definitions


def build_sw2021_industry_definitions(classify_df: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    l1_classify = classify_df[classify_df["level"].eq("L1")].copy()
    l2_classify = classify_df[classify_df["level"].eq("L2")].copy()
    l1_classify.sort_values(["industry_name", "index_code"], inplace=True)
    l2_classify.sort_values(["industry_name", "index_code"], inplace=True)

    parent_name_map = {
        str(row["industry_code"]).strip(): str(row["industry_name"]).strip()
        for _, row in l1_classify.iterrows()
    }

    l1_definitions = [
        {
            "ts_code": str(row["index_code"]).strip(),
            "industry_name": str(row["industry_name"]).strip(),
            "level": "L1",
            "parent_l1_name": None,
        }
        for _, row in l1_classify.iterrows()
    ]
    l2_definitions = [
        {
            "ts_code": str(row["index_code"]).strip(),
            "industry_name": str(row["industry_name"]).strip(),
            "level": "L2",
            "parent_l1_name": parent_name_map.get(str(row["parent_code"]).strip()),
        }
        for _, row in l2_classify.iterrows()
    ]

    if not l1_definitions or not l2_definitions:
        raise ValueError("未能从 SW2021 分类中识别完整的申万一级/二级行业清单。")

    return l1_definitions, l2_definitions


def compute_industry_crowding_panel(
    sector_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
) -> pd.DataFrame:
    if sector_df.empty:
        raise ValueError("未获取到行业拥挤度历史数据。")

    panel = benchmark_df[["trade_date", "trade_date_dt", "benchmark_close", "benchmark_ret"]].copy().merge(
        sector_df[["trade_date", "close", "amount", "float_mv"]],
        on="trade_date",
        how="left",
    )
    panel.rename(
        columns={
            "close": "sector_close",
            "amount": "sector_amount",
            "float_mv": "sector_float_mv",
        },
        inplace=True,
    )
    panel.sort_values("trade_date_dt", inplace=True)
    panel.reset_index(drop=True, inplace=True)
    panel["sector_ret"] = panel["sector_close"].pct_change(fill_method=None)

    first_valid_index = panel["sector_close"].first_valid_index()
    if first_valid_index is None:
        raise ValueError("行业历史数据缺少有效收盘价。")

    sector_base = panel.loc[first_valid_index, "sector_close"]
    benchmark_base = panel.loc[first_valid_index, "benchmark_close"]
    panel["sector_nav"] = panel["sector_close"] / sector_base
    panel["benchmark_nav_for_sector"] = panel["benchmark_close"] / benchmark_base
    panel["excess_nav"] = panel["sector_nav"] / panel["benchmark_nav_for_sector"]
    panel.loc[panel.index < first_valid_index, ["sector_nav", "benchmark_nav_for_sector", "excess_nav"]] = np.nan

    panel["daily_turnover_value"] = panel["sector_amount"] / panel["sector_float_mv"].replace({0: pd.NA})
    panel.loc[panel["sector_float_mv"] <= 0, "daily_turnover_value"] = np.nan

    for window in CROWDING_EXCESS_NAV_BIAS_WINDOWS:
        panel[f"excess_nav_bias_{window}"] = panel["excess_nav"] / panel["excess_nav"].rolling(window).mean() - 1
    for window in CROWDING_EXCESS_MOMENTUM_WINDOWS:
        panel[f"excess_momentum_{window}"] = panel["excess_nav"] / panel["excess_nav"].shift(window) - 1
    for window in CROWDING_TURNOVER_WINDOWS:
        panel[f"turnover_{window}"] = panel["daily_turnover_value"].rolling(window).mean()
    for window in CROWDING_TURNOVER_BIAS_WINDOWS:
        panel[f"turnover_bias_{window}"] = (
            panel["daily_turnover_value"] / panel["daily_turnover_value"].rolling(window).mean() - 1
        )

    excess_nav_bias_hits = build_crowding_hit_columns(panel, "excess_nav_bias", CROWDING_EXCESS_NAV_BIAS_WINDOWS)
    excess_momentum_hits = build_crowding_hit_columns(panel, "excess_momentum", CROWDING_EXCESS_MOMENTUM_WINDOWS)
    turnover_hits = build_crowding_hit_columns(panel, "turnover", CROWDING_TURNOVER_WINDOWS)
    turnover_bias_hits = build_crowding_hit_columns(panel, "turnover_bias", CROWDING_TURNOVER_BIAS_WINDOWS)

    panel = score_crowding_group(panel, excess_nav_bias_hits, "score_excess_nav_bias", 2)
    panel = score_crowding_group(panel, excess_momentum_hits, "score_excess_momentum", 2)
    panel = score_crowding_group(panel, turnover_hits, "score_turnover", 3)
    panel = score_crowding_group(panel, turnover_bias_hits, "score_turnover_bias", 2)

    score_columns = [
        "score_excess_nav_bias",
        "score_excess_momentum",
        "score_turnover",
        "score_turnover_bias",
    ]
    valid_score = panel[score_columns].notna().all(axis=1)
    panel["total_score"] = np.where(valid_score, panel[score_columns].fillna(0).sum(axis=1), np.nan)
    panel["high_crowding"] = panel["total_score"].fillna(-1).isin([3, 4])
    panel["crowded_20d_count"] = panel["high_crowding"].astype(int).rolling(20, min_periods=1).sum()
    panel["excluded_signal"] = panel["crowded_20d_count"] >= 2
    return panel


def sort_crowding_industries(industries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        industries,
        key=lambda item: (
            -int(bool(item["high_crowding"])),
            -int(bool(item["excluded_signal"])),
            -int(item["total_score"]),
            item["industry_name"],
        ),
    )


def build_consumer_crowdedness_groups(
    pro: Any,
    classify_df: pd.DataFrame,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    benchmark_df = fetch_index_daily(
        pro,
        CROWDING_BENCHMARK_CODE,
        lookback_days=CROWDING_HISTORY_LOOKBACK_DAYS,
    )
    if benchmark_df.empty:
        raise ValueError(f"未获取到拥挤度基准指数 {CROWDING_BENCHMARK_CODE} 的历史数据。")

    benchmark_df = benchmark_df.copy()
    benchmark_df.rename(columns={"close": "benchmark_close"}, inplace=True)
    benchmark_df["benchmark_ret"] = benchmark_df["benchmark_close"].pct_change()

    l1_definitions, l2_definitions = build_consumer_crowding_definitions(classify_df)
    panels: list[tuple[dict[str, Any], pd.DataFrame]] = []
    failed_industries: list[dict[str, str]] = []
    common_trade_dates: set[str] | None = None

    for industry in [*l1_definitions, *l2_definitions]:
        try:
            sector_df = fetch_sw_daily(
                pro,
                industry["ts_code"],
                lookback_days=CROWDING_HISTORY_LOOKBACK_DAYS,
            )
            panel = compute_industry_crowding_panel(sector_df, benchmark_df)
            valid_panel = panel[panel["total_score"].notna()].copy()
            if valid_panel.empty:
                raise ValueError(f"{industry['industry_name']} 未能生成有效拥挤度得分。")

            trade_dates = set(valid_panel["trade_date"].tolist())
            common_trade_dates = trade_dates if common_trade_dates is None else common_trade_dates & trade_dates
            panels.append((industry, valid_panel))
        except Exception as exc:
            failed_industries.append(
                {
                    "ts_code": str(industry["ts_code"]),
                    "industry_name": str(industry["industry_name"]),
                    "reason": str(exc),
                }
            )

    if not panels:
        if failed_industries:
            raise ValueError(failed_industries[0]["reason"])
        raise ValueError("未获取到行业拥挤度历史数据。")

    if not common_trade_dates:
        raise ValueError("消费行业拥挤度未找到共同的有效快照日期。")

    snapshot_trade_date = max(common_trade_dates)
    group_payload = {"l1": [], "l2": []}
    for industry, panel in panels:
        snapshot_row = panel[panel["trade_date"].eq(snapshot_trade_date)].iloc[-1]
        record = {
            "ts_code": industry["ts_code"],
            "industry_name": industry["industry_name"],
            "level": industry["level"],
            "parent_l1_name": industry["parent_l1_name"],
            "total_score": int(snapshot_row["total_score"]),
            "high_crowding": bool(snapshot_row["high_crowding"]),
            "excluded_signal": bool(snapshot_row["excluded_signal"]),
            "crowded_20d_count": int(snapshot_row["crowded_20d_count"]),
        }
        group_payload["l1" if industry["level"] == "L1" else "l2"].append(record)

    as_of_date = to_iso_date(snapshot_trade_date)
    if as_of_date is None:
        raise ValueError(f"无法解析拥挤度快照日期：{snapshot_trade_date}")

    return [
        {
            "group_id": "l1",
            "title": "申万一级行业",
            "as_of_date": as_of_date,
            "industries": sort_crowding_industries(group_payload["l1"]),
        },
        {
            "group_id": "l2",
            "title": "申万二级行业",
            "as_of_date": as_of_date,
            "industries": sort_crowding_industries(group_payload["l2"]),
        },
    ], failed_industries


def build_market_pb_roe_scatter_chart(
    pro: Any,
    definitions: list[dict[str, Any]],
    consumer_codes: set[str],
    roe_frames: dict[str, pd.DataFrame],
    roe_periods: list[str],
    chart_id: str,
    title: str,
    description: str,
) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    points: list[dict[str, Any]] = []
    failed_industries: list[dict[str, str]] = []
    latest_pb_dates: list[str] = []
    latest_roe_periods: list[str] = []

    for industry in definitions:
        try:
            price_df = fetch_sw_daily(pro, industry["ts_code"], lookback_days=PB_ROE_SCATTER_LOOKBACK_DAYS)
            if price_df.empty:
                raise ValueError("未获取到行业日频数据。")

            latest_pb, latest_pb_date, pb_percentile_5y = build_pb_scatter_metrics(price_df)
            if latest_pb is None or latest_pb_date is None or pb_percentile_5y is None:
                raise ValueError("PB 五年分位缺失。")

            constituents = current_constituents(pro, industry["ts_code"])
            if not constituents:
                raise ValueError("未获取到当前成分股。")

            _, latest_roe, latest_roe_period, roe_percentile_5y = build_roe_history(constituents, roe_frames, roe_periods)
            latest_roe_period_iso = to_iso_date(latest_roe_period) if latest_roe_period else None
            if latest_roe is None or latest_roe_period_iso is None or roe_percentile_5y is None:
                raise ValueError("ROE 五年分位缺失。")

            latest_pb_dates.append(latest_pb_date)
            latest_roe_periods.append(latest_roe_period_iso)
            points.append(
                {
                    "ts_code": industry["ts_code"],
                    "industry_name": industry["industry_name"],
                    "level": industry["level"],
                    "parent_l1_name": industry["parent_l1_name"],
                    "pb_percentile_5y": pb_percentile_5y,
                    "roe_percentile_5y": roe_percentile_5y,
                    "latest_pb": latest_pb,
                    "latest_pb_date": latest_pb_date,
                    "latest_roe": latest_roe,
                    "latest_roe_period": latest_roe_period_iso,
                    "is_consumer_related": industry["ts_code"] in consumer_codes,
                }
            )
        except Exception as exc:
            failed_industries.append(
                {
                    "ts_code": str(industry["ts_code"]),
                    "industry_name": str(industry["industry_name"]),
                    "reason": str(exc),
                }
            )

    if not points:
        return None, failed_industries

    latest_pb_text = max(latest_pb_dates) if latest_pb_dates else "未知"
    latest_roe_text = max(latest_roe_periods) if latest_roe_periods else "未知"
    return (
        {
            "chart_id": chart_id,
            "title": title,
            "description": description,
            "chart_type": "scatter",
            "x_axis_label": "PB五年分位数",
            "y_axis_label": "ROE五年分位数",
            "latest_label": "数据截至",
            "latest_time_text": f"PB {latest_pb_text} / ROE {latest_roe_text}",
            "points": points,
        },
        failed_industries,
    )


def build_consumer_pb_roe_section(
    pro: Any,
    classify_df: pd.DataFrame,
    roe_frames: dict[str, pd.DataFrame],
    roe_periods: list[str],
) -> dict[str, Any]:
    all_l1_definitions, _all_l2_definitions = build_sw2021_industry_definitions(classify_df)
    consumer_l1_codes = set(CONSUMER_MARKET_INDUSTRY_CODES)
    _, consumer_l2_definitions = build_consumer_crowding_definitions(classify_df)
    consumer_l2_codes = {item["ts_code"] for item in consumer_l2_definitions}

    l1_chart, l1_failed = build_market_pb_roe_scatter_chart(
        pro=pro,
        definitions=all_l1_definitions,
        consumer_codes=consumer_l1_codes,
        roe_frames=roe_frames,
        roe_periods=roe_periods,
        chart_id="consumer-pb-roe-l1",
        title="全部申万一级行业 PB-ROE 图",
        description="展示全部申万一级行业当前 PB 与 ROE 的五年历史分位，红点高亮消费相关行业。",
    )
    l2_chart, l2_failed = build_market_pb_roe_scatter_chart(
        pro=pro,
        definitions=consumer_l2_definitions,
        consumer_codes=consumer_l2_codes,
        roe_frames=roe_frames,
        roe_periods=roe_periods,
        chart_id="consumer-pb-roe-l2",
        title="消费相关申万二级行业 PB-ROE 图",
        description="展示消费相关申万二级子行业当前 PB 与 ROE 的五年历史分位，并用标签标注行业名称。",
    )

    charts = [chart for chart in [l1_chart, l2_chart] if chart is not None]
    if not charts:
        if l1_failed:
            raise ValueError(l1_failed[0]["reason"])
        if l2_failed:
            raise ValueError(l2_failed[0]["reason"])
        raise ValueError("未生成任何 PB-ROE 散点图。")

    failed_total = len(l1_failed) + len(l2_failed)
    notice_message = None
    if failed_total:
        notice_message = (
            f"部分行业本次未纳入散点图：一级 {len(l1_failed)} 个，二级 {len(l2_failed)} 个。"
        )

    return build_ready_market_section(
        section_id="consumer-pb-roe",
        title="消费相关行业 PB-ROE 图",
        description="通过标签切换查看全部申万一级行业与消费相关申万二级行业的 PB-ROE 散点图。",
        charts=charts,
        notice_message=notice_message,
    )


def build_broker_coverage_summary(month_rows: list[dict[str, Any]], latest_month: str) -> dict[str, Any]:
    broker_counts = [row["broker_count"] for row in month_rows]
    broker_count_consistent = len(set(broker_counts)) == 1
    broker_sets = {row["month"]: frozenset(row["brokers"]) for row in month_rows}
    latest_set = broker_sets[latest_month]
    broker_members_consistent = len(set(broker_sets.values())) == 1
    different_months_vs_latest = sum(1 for month, brokers in broker_sets.items() if month != latest_month and brokers != latest_set)

    if broker_count_consistent and broker_members_consistent:
        comparison_note = "样本月度间券商家数与券商名单保持一致。"
    else:
        parts: list[str] = []
        if not broker_count_consistent:
            parts.append(f"样本月度间券商家数不一致，范围为 {min(broker_counts)}-{max(broker_counts)} 家。")
        if not broker_members_consistent:
            parts.append(
                f"与最新月份 {month_label(latest_month)} 相比，有 {different_months_vs_latest} 个月的券商名单不同。"
            )
        parts.append("因此该占比更适合做趋势跟踪，不宜直接做严格横向比较。")
        comparison_note = "".join(parts)

    latest_row = next(row for row in month_rows if row["month"] == latest_month)
    return {
        "latest_month": month_label(latest_month),
        "months_covered": len(month_rows),
        "latest_broker_count": latest_row["broker_count"],
        "min_broker_count": min(broker_counts),
        "max_broker_count": max(broker_counts),
        "broker_count_consistent": broker_count_consistent,
        "broker_members_consistent": broker_members_consistent,
        "latest_total_recommendations": latest_row["total_recommendations"],
        "latest_consumer_recommendations": latest_row["consumer_recommendations"],
        "latest_consumer_stock_count": latest_row["consumer_stock_count"],
        "latest_consumer_share_pct": round(latest_row["consumer_share_pct"], 2),
        "comparison_note": comparison_note,
    }


def build_consumer_crowdedness_section(pro: Any, classify_df: pd.DataFrame) -> dict[str, Any]:
    crowdedness_groups, failed_industries = build_consumer_crowdedness_groups(pro, classify_df)
    notice_message = None
    if failed_industries:
        notice_message = f"部分行业本次未更新，已展示其余可用行业。未更新行业 {len(failed_industries)} 个。"
    return build_ready_market_section(
        section_id="consumer-crowdedness",
        title="消费相关行业拥挤度跟踪",
        description="测算行业拥挤度，并展示消费相关申万一级与二级行业当前拥挤度得分，以及即时预警和排除信号。",
        notice_message=notice_message,
        crowdedness_groups=crowdedness_groups,
    )


def build_broker_consumer_share_section(pro: Any, classify_df: pd.DataFrame) -> dict[str, Any]:
    history = fetch_broker_recommend_history(pro)
    stock_to_industry = build_consumer_stock_industry_map(pro, classify_df)
    month_rows: list[dict[str, Any]] = []

    for month, month_df in history.groupby("month", sort=True):
        broker_names = sorted({broker for broker in month_df["broker"].tolist() if broker})
        consumer_df = month_df[month_df["ts_code"].isin(stock_to_industry)].copy()
        consumer_stock_count = consumer_df["ts_code"].nunique()
        total_recommendations = len(month_df)
        consumer_recommendations = len(consumer_df)
        consumer_share_pct = round(consumer_recommendations / total_recommendations * 100, 2) if total_recommendations else 0.0
        month_rows.append(
            {
                "month": month,
                "brokers": broker_names,
                "broker_count": len(broker_names),
                "total_recommendations": total_recommendations,
                "consumer_recommendations": consumer_recommendations,
                "consumer_stock_count": consumer_stock_count,
                "consumer_share_pct": consumer_share_pct,
            }
        )

    if not month_rows:
        raise ValueError("券商荐股月度样本为空。")

    latest_month = month_rows[-1]["month"]
    previous_month = month_rows[-2]["month"] if len(month_rows) > 1 else None
    latest_month_df = history[history["month"].eq(latest_month) & history["ts_code"].isin(stock_to_industry)].copy()
    latest_month_df["industry_name"] = latest_month_df["ts_code"].map(stock_to_industry).fillna("消费行业")
    previous_month_counts: dict[str, int] = {}

    if previous_month is not None:
        previous_month_df = history[
            history["month"].eq(previous_month) & history["ts_code"].isin(stock_to_industry)
        ].copy()
        if not previous_month_df.empty:
            previous_month_counts = (
                previous_month_df.groupby("ts_code")["broker"]
                .nunique()
                .astype(int)
                .to_dict()
            )

    recommended_stocks: list[dict[str, Any]] = []
    if not latest_month_df.empty:
        for (ts_code, name, industry_name), group in latest_month_df.groupby(["ts_code", "name", "industry_name"], sort=False):
            brokers = sorted({broker for broker in group["broker"].tolist() if broker})
            current_count = len(brokers)
            previous_count = previous_month_counts.get(ts_code, 0)
            recommended_stocks.append(
                {
                    "ts_code": ts_code,
                    "name": name,
                    "industry_name": industry_name,
                    "broker_count": current_count,
                    "previous_broker_count": previous_count if previous_month is not None else None,
                    "broker_count_delta": current_count - previous_count if previous_month is not None else None,
                    "broker_names": brokers,
                }
            )

    recommended_stocks.sort(key=lambda item: (-item["broker_count"], item["ts_code"]))

    coverage = build_broker_coverage_summary(month_rows, latest_month)
    chart = build_macro_chart(
        chart_id="consumer-broker-share",
        title="券商金股中消费个股占比统计",
        description="统计 Tushare 券商每月荐股中，属于食品饮料、农林牧渔、美容护理、家用电器、商贸零售、社会服务、轻工制造和纺织服饰的个股占全部荐股条目的比例。",
        labels=[month_label(row["month"]) for row in month_rows],
        x_axis_label="时间",
        latest_time_text=month_label(latest_month),
        series=[
            {
                "series_id": "consumer-recommendation-share",
                "label": "消费个股占比",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": [round_or_none(row["consumer_share_pct"]) for row in month_rows],
            }
        ],
    )

    notice_message = None
    if not coverage["broker_count_consistent"] or not coverage["broker_members_consistent"]:
        notice_message = coverage["comparison_note"]

    return build_ready_market_section(
        section_id="consumer-broker-share",
        title="券商金股中消费个股占比统计",
        description="基于 Tushare 券商月度荐股数据统计消费个股占比，并展示最新月份消费个股及对应推荐券商。",
        chart=chart,
        notice_message=notice_message,
        coverage=coverage,
        recommended_stocks=recommended_stocks,
    )


def build_market_tracking_board(updated_at: str, stale_after_minutes: int) -> dict[str, Any]:
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        print("TUSHARE_TOKEN missing; reusing existing market-tracking JSON file.")
        return load_existing_market_tracking_output(updated_at, stale_after_minutes)

    pro = create_tushare_client(token)
    classify_df: pd.DataFrame | None = None
    classify_error: Exception | None = None
    roe_periods = quarter_end_candidates(ROE_HISTORY_QUARTERS)
    roe_frames: dict[str, pd.DataFrame] | None = None
    roe_error: Exception | None = None
    try:
        classify_df = fetch_sw2021_classify(pro)
    except Exception as exc:
        classify_error = exc
    try:
        roe_frames = fetch_roe_frames(pro, roe_periods)
    except Exception as exc:
        roe_error = exc
    sections: list[dict[str, Any]] = []
    ready_count = 0
    placeholder_count = 0
    error_count = 0

    try:
        if classify_df is None:
            raise ValueError(f"SW2021 分类加载失败：{classify_error}")
        if roe_frames is None:
            raise ValueError(f"ROE 财务数据加载失败：{roe_error}")
        sections.append(build_consumer_pb_roe_section(pro, classify_df, roe_frames, roe_periods))
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_market_section(
                section_id="consumer-pb-roe",
                title="消费相关行业 PB-ROE 图",
                description="通过标签切换查看全部申万一级行业与消费相关申万二级行业的 PB-ROE 散点图。",
                message=f"消费相关行业 PB-ROE 图暂未生成：{exc}",
            )
        )
        error_count += 1

    try:
        if classify_df is None:
            raise ValueError(f"SW2021 分类加载失败：{classify_error}")
        sections.append(build_consumer_crowdedness_section(pro, classify_df))
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_market_section(
                section_id="consumer-crowdedness",
                title="消费相关行业拥挤度跟踪",
                description="测算行业拥挤度，并展示消费相关申万一级与二级行业当前拥挤度得分，以及即时预警和排除信号。",
                message=f"消费相关行业拥挤度暂未生成：{exc}",
            )
        )
        error_count += 1

    try:
        if classify_df is None:
            raise ValueError(f"SW2021 分类加载失败：{classify_error}")
        sections.append(build_broker_consumer_share_section(pro, classify_df))
        ready_count += 1
    except Exception as exc:
        sections.append(
            build_error_market_section(
                section_id="consumer-broker-share",
                title="券商金股中消费个股占比统计",
                description="统计券商每月荐股中消费个股占全部荐股条目的比例，并展示最新月份消费个股及对应推荐券商。",
                message=f"券商金股消费占比统计暂未生成：{exc}",
            )
        )
        error_count += 1

    status = "ok"
    if error_count > 0 and ready_count == 0:
        status = "error"
    elif error_count > 0 or placeholder_count > 0:
        status = "partial"

    return {
        "updated_at": updated_at,
        "source": "Tushare sw_daily + fina_indicator_vip + index_daily(000985.CSI) + broker_recommend + index_member + index_classify(SW2021)",
        "stale_after_minutes": stale_after_minutes,
        "status": status,
        "status_message": f"已接入 {ready_count} 个市场跟踪栏目，{placeholder_count} 个栏目保留占位，{error_count} 个栏目加载失败。",
        "board_id": "market-tracking",
        "title": "市场跟踪",
        "description": "市场跟踪板块用于补充消费相关行业 PB-ROE、拥挤度和券商金股中消费个股占比观察。",
        "sections": sections,
    }


def build_second_hand_housing_section() -> dict[str, Any]:
    description = "从手动更新的 bingshan_index.xlsx 中重建二手房价格月环比和活跃度图表，每次替换文件后重新运行 ETL 即可同步更新。"
    charts: list[dict[str, Any]] = []
    errors: list[str] = []

    for label, loader in (
        ("二手房价格月环比", load_second_hand_price_mom_chart),
        ("二手房活跃度", load_second_hand_activity_chart),
    ):
        try:
            charts.append(loader())
        except Exception as exc:
            errors.append(f"{label}：{exc}")

    if charts:
        return build_ready_macro_section(
            section_id="second-hand-housing",
            title="二手房",
            description=description,
            charts=charts,
            message="；".join(errors) if errors else None,
        )

    return build_error_macro_section(
        section_id="second-hand-housing",
        title="二手房",
        description=description,
        message="二手房栏目暂未生成：" + "；".join(errors or ["未读取到可用数据"]),
    )


def build_new_home_section() -> dict[str, Any]:
    return build_placeholder_macro_section(
        section_id="new-home",
        title="新房",
        description="新房栏目后续将补充国家统计局口径的新建商品房销售额和 70 大中城市新房价格图表。",
        message="统计局数据暂时留空，后续再接入。",
    )


def load_new_home_sales_chart_v2() -> dict[str, Any]:
    chart_start_year = date.today().year - REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS
    source_start_year = chart_start_year - 1
    entries = fetch_new_home_sales_release_entries_v2(start_year=source_start_year)
    records: list[dict[str, Any]] = []
    for entry in entries:
        try:
            cumulative_value = extract_new_home_sales_amount(entry["url"])
        except Exception:
            continue

        records.append(
            {
                "year": entry["year"],
                "month": entry["month"],
                "label": f"{entry['year']}-{entry['month']:02d}",
                "date": pd.Timestamp(year=entry["year"], month=entry["month"], day=1),
                "cumulative_value": cumulative_value,
            }
        )

    if not records:
        raise ValueError("未抓取到新建商品房销售额数据。")

    records.sort(key=lambda item: (item["year"], item["month"]))
    cumulative_map = {
        (record["year"], record["month"]): record["cumulative_value"]
        for record in records
    }
    records_with_values: list[dict[str, Any]] = []

    for record in records:
        year = record["year"]
        month = record["month"]
        yoy_value: float | None = None

        if month == 2:
            prior_cumulative = cumulative_map.get((year - 1, month))
            if prior_cumulative not in (None, 0):
                yoy_value = round((record["cumulative_value"] / prior_cumulative - 1) * 100, 2)
        else:
            prior_cumulative = cumulative_map.get((year, month - 1))
            prior_year_cumulative = cumulative_map.get((year - 1, month))
            prior_year_prev_cumulative = cumulative_map.get((year - 1, month - 1))
            if (
                prior_cumulative is not None
                and prior_year_cumulative is not None
                and prior_year_prev_cumulative is not None
            ):
                current_month_value = record["cumulative_value"] - prior_cumulative
                prior_year_month_value = prior_year_cumulative - prior_year_prev_cumulative
                if current_month_value >= 0 and prior_year_month_value > 0:
                    yoy_value = round((current_month_value / prior_year_month_value - 1) * 100, 2)

        records_with_values.append(
            {
                "label": record["label"],
                "date": record["date"],
                "value": yoy_value,
            }
        )

    latest_date = records_with_values[-1]["date"]
    window_start = latest_date - pd.DateOffset(years=REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS)
    filtered_records = [
        record
        for record in records_with_values
        if record["date"] >= window_start and record["date"].year >= chart_start_year
    ]
    if not filtered_records:
        raise ValueError("未提取到滚动三年窗口内的新建商品房销售额同比数据。")

    labels = [record["label"] for record in filtered_records]
    values = [record["value"] for record in filtered_records]

    return build_macro_chart(
        chart_id="new-home-sales-monthly",
        title="新建商品房销售额",
        description="来自国家统计局月度房地产市场发布页。1-2 月展示累计销售额同比，其余月份按年内累计值拆分计算当月销售额同比，并统一展示最近滚动 3 年。",
        labels=labels,
        x_axis_label="月份",
        latest_time_text=labels[-1],
        series=[
            {
                "series_id": "new-home-sales-monthly",
                "label": "新建商品房销售额同比",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": False,
                "values": values,
            }
        ],
    )


def build_new_home_section_v2() -> dict[str, Any]:
    description = "展示国家统计局口径的新建商品房销售额和 70 大中城市新房价格，并统一保留最近滚动 3 年窗口。"
    charts: list[dict[str, Any]] = []
    errors: list[str] = []

    chart_loaders = (
        ("新建商品房销售额", load_new_home_sales_chart_v2),
        ("70 大中城市新房价格", load_new_home_price_tier_chart_v2),
    )
    for label, loader in chart_loaders:
        try:
            charts.append(loader())
        except Exception as exc:
            errors.append(f"{label}：{exc}")

    if charts:
        return build_ready_macro_section(
            section_id="new-home",
            title="新房",
            description=description,
            charts=charts,
            message="；".join(errors) if errors else None,
        )

    return build_error_macro_section(
        section_id="new-home",
        title="新房",
        description=description,
        message="新房栏目暂未生成：" + "；".join(errors or ["未读取到可用数据"]),
    )


def build_real_estate_high_frequency_board(updated_at: str, stale_after_minutes: int) -> dict[str, Any]:
    sections = [
        build_second_hand_housing_section(),
        build_new_home_section_v2(),
    ]

    ready_count = sum(1 for section in sections if section["status"] == "ready")
    placeholder_count = sum(1 for section in sections if section["status"] == "coming_soon")
    error_count = sum(1 for section in sections if section["status"] == "error")

    status = "ok"
    if error_count > 0 and ready_count == 0:
        status = "error"
    elif error_count > 0 or placeholder_count > 0:
        status = "partial"

    return {
        "updated_at": updated_at,
        "source": "input_data/bingshan_index.xlsx + 国家统计局",
        "stale_after_minutes": stale_after_minutes,
        "status": status,
        "status_message": f"已接入 {ready_count} 个栏目，待补充 {placeholder_count} 个栏目，加载失败 {error_count} 个栏目。",
        "board_id": "real-estate-high-frequency",
        "title": "地产高频",
        "description": "地产高频板块展示二手房高频跟踪，以及国家统计局口径的新建商品房销售额和 70 大中城市新房价格图表。",
        "sections": sections,
    }


def load_new_house_frame() -> pd.DataFrame:
    if not NEW_HOUSE_PATH.exists():
        raise FileNotFoundError(f"未找到手动维护的文件：{NEW_HOUSE_PATH}")

    frame = pd.read_excel(
        NEW_HOUSE_PATH,
        sheet_name=0,
        header=1,
        engine="openpyxl",
        na_values=["#N/A", "#N/A N/A"],
    )
    frame = frame.iloc[:, :5].copy()
    frame.columns = ["date", "price_yoy", "price_mom", "sales_area_cum", "sales_amount_cum"]
    frame["date"] = pd.to_datetime(frame["date"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    if frame["date"].isna().all():
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")

    for column in ["price_yoy", "price_mom", "sales_area_cum", "sales_amount_cum"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame[frame["date"].notna()].copy()
    frame.sort_values("date", inplace=True)
    frame.drop_duplicates(subset=["date"], keep="last", inplace=True)
    frame.reset_index(drop=True, inplace=True)

    if frame.empty:
        raise ValueError("new_house.xlsx 未读取到有效月度数据。")

    return frame


def add_monthly_and_yoy_columns(frame: pd.DataFrame, cumulative_column: str, prefix: str) -> pd.DataFrame:
    monthly_values: list[float | None] = []
    cumulative_by_period: dict[tuple[int, int], float | None] = {}

    for row in frame.itertuples(index=False):
        date_value = row.date
        cumulative_value = getattr(row, cumulative_column)
        monthly_value: float | None = None

        if cumulative_value is not None and not pd.isna(cumulative_value):
            numeric_cumulative = float(cumulative_value)
            if date_value.month <= 2:
                monthly_value = numeric_cumulative
            else:
                previous_cumulative = cumulative_by_period.get((date_value.year, date_value.month - 1))
                if previous_cumulative is not None:
                    monthly_value = numeric_cumulative - previous_cumulative

            cumulative_by_period[(date_value.year, date_value.month)] = numeric_cumulative
        else:
            cumulative_by_period[(date_value.year, date_value.month)] = None

        monthly_values.append(monthly_value)

    monthly_map = {
        (row.date.year, row.date.month): monthly_value
        for row, monthly_value in zip(frame.itertuples(index=False), monthly_values, strict=True)
    }
    yoy_values: list[float | None] = []
    for row, monthly_value in zip(frame.itertuples(index=False), monthly_values, strict=True):
        prior_year_value = monthly_map.get((row.date.year - 1, row.date.month))
        yoy_value: float | None = None
        if monthly_value is not None and prior_year_value not in (None, 0):
            yoy_value = round((monthly_value / prior_year_value - 1) * 100, 2)
        yoy_values.append(yoy_value)

    result = frame.copy()
    result[f"{prefix}_monthly"] = monthly_values
    result[f"{prefix}_yoy"] = yoy_values
    return result


def load_new_home_price_tier_chart_v2() -> dict[str, Any]:
    frame = load_new_house_frame()
    latest_date = frame["date"].iloc[-1]
    window_start = latest_date - pd.DateOffset(years=REAL_ESTATE_HIGH_FREQUENCY_NEW_HOME_ROLLING_YEARS)
    filtered = frame[frame["date"] >= window_start].copy()
    if filtered.empty:
        raise ValueError("未读取到最近滚动 5 年的新房价格数据。")

    labels = filtered["date"].dt.strftime("%Y-%m").tolist()
    return build_macro_chart(
        chart_id="new-home-price-tier",
        title="70大中城市新房价格同环比",
        description="70 大中城市新建商品住宅价格当月同比与环比，滚动5年展示。",
        labels=labels,
        x_axis_label="月份",
        latest_time_text=labels[-1],
        chart_type="combo",
        series=[
            {
                "series_id": "price_yoy",
                "label": "同比",
                "unit": "%",
                "y_axis_id": "left",
                "render_type": "line",
                "span_gaps": False,
                "show_point_markers": True,
                "values": series_to_list(filtered["price_yoy"]),
            },
            {
                "series_id": "price_mom",
                "label": "环比",
                "unit": "%",
                "y_axis_id": "right",
                "render_type": "bar",
                "span_gaps": False,
                "show_point_markers": False,
                "values": series_to_list(filtered["price_mom"]),
            },
        ],
    )


def load_new_home_sales_chart_v2() -> dict[str, Any]:
    frame = load_new_house_frame()
    frame = add_monthly_and_yoy_columns(frame, "sales_area_cum", "sales_area")
    frame = add_monthly_and_yoy_columns(frame, "sales_amount_cum", "sales_amount")

    latest_date = frame["date"].iloc[-1]
    window_start = latest_date - pd.DateOffset(years=REAL_ESTATE_HIGH_FREQUENCY_NEW_HOME_ROLLING_YEARS)
    filtered = frame[frame["date"] >= window_start].copy()
    if filtered.empty:
        raise ValueError("未读取到最近滚动 5 年的新房销售同比数据。")

    labels = filtered["date"].dt.strftime("%Y-%m").tolist()
    return build_macro_chart(
        chart_id="new-home-sales-monthly",
        title="新房销售面积和金额同比",
        description="新建商品住宅销售面积和销售金额当月同比，滚动5年展示。",
        labels=labels,
        x_axis_label="月份",
        latest_time_text=labels[-1],
        series=[
            {
                "series_id": "sales_area_yoy",
                "label": "销售面积同比",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": True,
                "values": series_to_list(filtered["sales_area_yoy"]),
            },
            {
                "series_id": "sales_amount_yoy",
                "label": "销售金额同比",
                "unit": "%",
                "y_axis_id": "left",
                "span_gaps": True,
                "values": series_to_list(filtered["sales_amount_yoy"]),
            },
        ],
    )


def build_new_home_section_v2() -> dict[str, Any]:
    description = "展示基于手动维护的 new_house.xlsx 生成的 70 大中城市新房价格同环比，以及新房销售面积和金额同比，并统一保留最近滚动 5 年窗口。"
    charts: list[dict[str, Any]] = []
    errors: list[str] = []

    chart_loaders = (
        ("70 大中城市新房价格同环比", load_new_home_price_tier_chart_v2),
        ("新房销售面积和金额同比", load_new_home_sales_chart_v2),
    )
    for label, loader in chart_loaders:
        try:
            charts.append(loader())
        except Exception as exc:
            errors.append(f"{label}：{exc}")

    if charts:
        return build_ready_macro_section(
            section_id="new-home",
            title="新房",
            description=description,
            charts=charts,
            message="；".join(errors) if errors else None,
        )

    return build_error_macro_section(
        section_id="new-home",
        title="新房",
        description=description,
        message="新房栏目暂未生成：" + "；".join(errors or ["未读取到可用数据"]),
    )


def build_real_estate_high_frequency_board(updated_at: str, stale_after_minutes: int) -> dict[str, Any]:
    sections = [
        build_second_hand_housing_section(),
        build_new_home_section_v2(),
    ]

    ready_count = sum(1 for section in sections if section["status"] == "ready")
    placeholder_count = sum(1 for section in sections if section["status"] == "coming_soon")
    error_count = sum(1 for section in sections if section["status"] == "error")

    status = "ok"
    if error_count > 0 and ready_count == 0:
        status = "error"
    elif error_count > 0 or placeholder_count > 0:
        status = "partial"

    return {
        "updated_at": updated_at,
        "source": "input_data/bingshan_index.xlsx + input_data/new_house.xlsx",
        "stale_after_minutes": stale_after_minutes,
        "status": status,
        "status_message": f"已接入 {ready_count} 个栏目，待补充 {placeholder_count} 个栏目，加载失败 {error_count} 个栏目。",
        "board_id": "real-estate-high-frequency",
        "title": "地产高频",
        "description": "地产高频板块展示二手房高频跟踪，以及基于手动维护文件生成的新房价格与销售同比图表。",
        "sections": sections,
    }


def build_outputs(
    config: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    panels_payload, series_payload = build_sector_outputs(config)
    macro_payload = build_macro_board_v2(
        updated_at=now_iso(),
        stale_after_minutes=config.get("stale_after_minutes", DEFAULT_STALE_AFTER_MINUTES),
    )
    real_estate_high_frequency_payload = build_real_estate_high_frequency_board(
        updated_at=now_iso(),
        stale_after_minutes=config.get("stale_after_minutes", DEFAULT_STALE_AFTER_MINUTES),
    )
    market_tracking_payload = build_market_tracking_board(
        updated_at=now_iso(),
        stale_after_minutes=config.get("stale_after_minutes", DEFAULT_STALE_AFTER_MINUTES),
    )
    return panels_payload, series_payload, macro_payload, real_estate_high_frequency_payload, market_tracking_payload


def main() -> None:
    ensure_dir(PUBLIC_DATA_DIR)
    config = load_json(CONFIG_PATH)
    (
        panels_payload,
        series_payload,
        macro_payload,
        real_estate_high_frequency_payload,
        market_tracking_payload,
    ) = build_outputs(config)

    outputs = {
        "sector-market-panels.json": panels_payload,
        "sector-market-series.json": series_payload,
        "macro-board.json": macro_payload,
        "real-estate-high-frequency-board.json": real_estate_high_frequency_payload,
        "market-tracking-board.json": market_tracking_payload,
    }

    for filename, payload in outputs.items():
        write_json(PUBLIC_DATA_DIR / filename, payload)

    print("Generated data files:")
    for filename in outputs:
        print(f"- public/data/{filename}")


if __name__ == "__main__":
    main()
