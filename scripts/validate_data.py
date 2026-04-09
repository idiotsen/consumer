from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DATA_DIR = ROOT / "public" / "data"
SCHEMA_DIR = ROOT / "schemas"
CONFIG_PATH = ROOT / "data_sources" / "raw" / "sector_board_config.json"
EXPECTED_MACRO_SECTION_IDS = [
    "inflation",
    "income",
    "household-balance-sheet",
    "household-consumption-intention",
]
EXPECTED_REAL_ESTATE_HIGH_FREQUENCY_SECTION_IDS = [
    "second-hand-housing",
    "new-home",
]
REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS = 3
REAL_ESTATE_HIGH_FREQUENCY_NEW_HOME_ROLLING_YEARS = 5
EXPECTED_MARKET_SECTION_IDS = [
    "consumer-pb-roe",
    "consumer-crowdedness",
    "consumer-broker-share",
]
EXPECTED_CONSUMER_CROWDING_L1_NAMES = {
    "食品饮料",
    "农林牧渔",
    "美容护理",
    "家用电器",
    "商贸零售",
    "社会服务",
    "轻工制造",
    "纺织服饰",
}
EXPECTED_CONSUMER_CROWDING_L1_COUNT = 8
EXPECTED_CONSUMER_CROWDING_L2_COUNT = 35
EXPECTED_SW2021_L1_COUNT = 31


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def validate_schema(data_filename: str, schema_filename: str) -> dict:
    data = load_json(PUBLIC_DATA_DIR / data_filename)
    schema = load_json(SCHEMA_DIR / schema_filename)
    validator = Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(data), key=lambda error: error.path)
    if errors:
        messages = []
        for error in errors:
            location = ".".join(str(part) for part in error.path) or "<root>"
            messages.append(f"{data_filename}: {location}: {error.message}")
        raise SystemExit("\n".join(messages))
    return data


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def parse_date(value: str) -> datetime:
    return datetime.fromisoformat(value)


def parse_quarter_label(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})Q([1-4])", value)
    if match is None:
        raise SystemExit(f"Invalid quarter label: {value}")
    return int(match.group(1)), int(match.group(2))


def quarter_to_datetime(year: int, quarter: int) -> datetime:
    month = quarter * 3
    return datetime(year, month, 1)


def validate_common(payload: dict, filename: str) -> None:
    parse_iso(payload["updated_at"])
    if payload["stale_after_minutes"] <= 0:
        raise SystemExit(f"{filename} stale_after_minutes must be > 0")


def ensure_percentile(value: float | None, label: str) -> None:
    if value is None:
        return
    if not 0 <= value <= 100:
        raise SystemExit(f"{label} must be between 0 and 100, got {value}")


def validate_panels(panels: dict, config: dict) -> set[tuple[str, str]]:
    expected_board_ids = [board["board_id"] for board in config["boards"]]
    actual_board_ids = [board["board_id"] for board in panels["boards"]]
    if actual_board_ids != expected_board_ids:
        raise SystemExit("Board order does not match sector_board_config.json")

    panel_lookup: set[tuple[str, str]] = set()
    enabled_ready_or_partial: set[tuple[str, str]] = set()

    for board in panels["boards"]:
        seen_codes = set()
        for industry in board["industries"]:
            key = (board["board_id"], industry["ts_code"])
            if key in panel_lookup:
                raise SystemExit(f"Duplicate industry panel: {board['board_id']} / {industry['ts_code']}")
            panel_lookup.add(key)

            if industry["ts_code"] in seen_codes:
                raise SystemExit(f"Duplicate ts_code within board {board['board_id']}: {industry['ts_code']}")
            seen_codes.add(industry["ts_code"])

            status = industry["status"]
            if status == "coming_soon":
                forbidden_fields = [
                    "latest_close",
                    "latest_pct_change",
                    "pb",
                    "amount",
                    "float_mv",
                    "member_count",
                    "pb_percentiles",
                    "crowdedness",
                    "pb_roe_alignment",
                ]
                leaked = [field for field in forbidden_fields if field in industry]
                if leaked:
                    raise SystemExit(
                        f"Coming soon industry must not expose numeric diagnostics: {board['board_id']} / {industry['ts_code']} -> {', '.join(leaked)}"
                    )
                continue

            if status not in {"ready", "partial", "error"}:
                raise SystemExit(f"Unexpected industry status: {status}")

            if status in {"ready", "partial"}:
                enabled_ready_or_partial.add(key)
                if "latest_trade_date" not in industry:
                    raise SystemExit(f"Missing latest_trade_date for {board['board_id']} / {industry['ts_code']}")
                parse_date(industry["latest_trade_date"])

                pb_percentiles = industry["pb_percentiles"]
                ensure_percentile(pb_percentiles["three_year"], f"{industry['ts_code']} pb three_year")
                ensure_percentile(pb_percentiles["five_year"], f"{industry['ts_code']} pb five_year")

                crowdedness = industry["crowdedness"]
                ensure_percentile(crowdedness["amount_pct"], f"{industry['ts_code']} crowdedness amount_pct")
                ensure_percentile(crowdedness["turnover_ratio_pct"], f"{industry['ts_code']} crowdedness turnover_ratio_pct")
                ensure_percentile(crowdedness["score"], f"{industry['ts_code']} crowdedness score")

                alignment = industry["pb_roe_alignment"]
                ensure_percentile(alignment["pb_pct_3y"], f"{industry['ts_code']} alignment pb_pct_3y")
                ensure_percentile(alignment["pb_pct_5y"], f"{industry['ts_code']} alignment pb_pct_5y")
                ensure_percentile(alignment["roe_pct_20q"], f"{industry['ts_code']} alignment roe_pct_20q")

                if alignment["current_roe_period"]:
                    parse_date(alignment["current_roe_period"])
                if alignment["match_gap"] is not None and not -100 <= alignment["match_gap"] <= 100:
                    raise SystemExit(f"{industry['ts_code']} match_gap must be between -100 and 100")
                if alignment["status"] == "empty" and alignment["label"] != "数据不足":
                    raise SystemExit(f"{industry['ts_code']} empty alignment must use 数据不足 label")

    expected_panel_keys = {
        (board["board_id"], industry["ts_code"])
        for board in config["boards"]
        for industry in board["industries"]
    }
    if panel_lookup != expected_panel_keys:
        raise SystemExit("Panel output does not match sector_board_config.json industries")

    return enabled_ready_or_partial


def validate_series(series_payload: dict, valid_industry_keys: set[tuple[str, str]]) -> None:
    seen_keys = set()
    for series in series_payload["series"]:
        key = (series["board_id"], series["ts_code"])
        if key in seen_keys:
            raise SystemExit(f"Duplicate series entry: {series['board_id']} / {series['ts_code']}")
        seen_keys.add(key)

        if key not in valid_industry_keys:
            raise SystemExit(f"Series emitted for non-ready industry: {series['board_id']} / {series['ts_code']}")

        lengths = {
            "dates": len(series["dates"]),
            "close": len(series["close"]),
            "normalized_close": len(series["normalized_close"]),
            "benchmark_close": len(series["benchmark_close"]),
            "benchmark_normalized_close": len(series["benchmark_normalized_close"]),
            "relative_to_benchmark": len(series["relative_to_benchmark"]),
            "pb": len(series["pb"]),
            "amount": len(series["amount"]),
            "turnover_value_ratio": len(series["turnover_value_ratio"]),
        }
        if len(set(lengths.values())) != 1:
            raise SystemExit(f"Series length mismatch for {series['board_id']} / {series['ts_code']}")
        if lengths["dates"] == 0:
            raise SystemExit(f"Series must not be empty: {series['board_id']} / {series['ts_code']}")

        previous_date = None
        for value in series["dates"]:
            parse_date(value)
            if previous_date and value <= previous_date:
                raise SystemExit(f"Series dates must be strictly ascending: {series['board_id']} / {series['ts_code']}")
            previous_date = value

        first_normalized = series["normalized_close"][0]
        if first_normalized is not None and abs(first_normalized - 100) > 0.2:
            raise SystemExit(f"First normalized_close must start near 100: {series['board_id']} / {series['ts_code']}")

        first_benchmark_normalized = series["benchmark_normalized_close"][0]
        if first_benchmark_normalized is not None and abs(first_benchmark_normalized - 100) > 0.2:
            raise SystemExit(
                f"First benchmark_normalized_close must start near 100: {series['board_id']} / {series['ts_code']}"
            )

        first_relative = series["relative_to_benchmark"][0]
        if first_relative is not None and abs(first_relative - 100) > 0.2:
            raise SystemExit(
                f"First relative_to_benchmark must start near 100: {series['board_id']} / {series['ts_code']}"
            )

    if seen_keys != valid_industry_keys:
        missing = valid_industry_keys - seen_keys
        raise SystemExit(f"Missing series for ready/partial industries: {sorted(missing)}")


def validate_macro_chart(chart: dict, section_id: str) -> None:
    chart_id = chart["chart_id"]
    labels = chart["labels"]
    if not labels:
        raise SystemExit(f"{section_id} / {chart_id} chart must contain labels")
    if not chart["latest_time_text"]:
        raise SystemExit(f"{section_id} / {chart_id} chart latest_time_text must not be empty")
    if not chart["series"]:
        raise SystemExit(f"{section_id} / {chart_id} chart must contain at least one series")
    if "show_point_markers" in chart and not isinstance(chart["show_point_markers"], bool):
        raise SystemExit(f"{section_id} / {chart_id} show_point_markers must be boolean")

    latest_changes = chart.get("latest_changes", [])
    seen_change_labels = set()
    for item in latest_changes:
        if item["direction"] not in {"up", "down", "flat"}:
            raise SystemExit(f"{section_id} / {chart_id} latest_changes direction must be up/down/flat")
        if item["label"] in seen_change_labels:
            raise SystemExit(f"{section_id} / {chart_id} latest_changes labels must be unique")
        seen_change_labels.add(item["label"])

    previous_label = None
    for label in labels:
        if previous_label and label <= previous_label:
            raise SystemExit(f"{section_id} / {chart_id} chart labels must be strictly ascending")
        previous_label = label

    for series in chart["series"]:
        if len(series["values"]) != len(labels):
            raise SystemExit(f"{section_id} / {chart_id} / {series['series_id']} values length must match labels length")
        if series["y_axis_id"] not in {"left", "right"}:
            raise SystemExit(f"{section_id} / {chart_id} / {series['series_id']} y_axis_id must be left or right")
        if not isinstance(series["span_gaps"], bool):
            raise SystemExit(f"{section_id} / {chart_id} / {series['series_id']} span_gaps must be boolean")
        if "show_point_markers" in series and not isinstance(series["show_point_markers"], bool):
            raise SystemExit(f"{section_id} / {chart_id} / {series['series_id']} show_point_markers must be boolean")
        if "render_type" in series and series["render_type"] not in {"line", "bar"}:
            raise SystemExit(f"{section_id} / {chart_id} / {series['series_id']} render_type must be line or bar")
        if "stack" in series and not isinstance(series["stack"], str):
            raise SystemExit(f"{section_id} / {chart_id} / {series['series_id']} stack must be string")

    if chart_id == "household-balance-sheet":
        if chart["chart_type"] != "combo":
            raise SystemExit("household-balance-sheet chart must use combo chart_type")

        quarter_keys = [parse_quarter_label(label) for label in labels]
        expected_quarter_keys = []
        year, quarter = quarter_keys[0]
        while (year, quarter) <= quarter_keys[-1]:
            expected_quarter_keys.append((year, quarter))
            if quarter == 4:
                year += 1
                quarter = 1
            else:
                quarter += 1
        if quarter_keys != expected_quarter_keys:
            raise SystemExit("household-balance-sheet chart labels must be continuous quarters")
        if chart["latest_time_text"] != labels[-1]:
            raise SystemExit("household-balance-sheet latest_time_text must equal the last quarter label")

        expected_ids = ["housing", "stock", "deposit", "total"]
        actual_ids = [series["series_id"] for series in chart["series"]]
        if actual_ids != expected_ids:
            raise SystemExit("household-balance-sheet chart must expose housing/stock/deposit/total series in order")

        series_map = {series["series_id"]: series for series in chart["series"]}
        bar_stack = None
        for series_id in ["housing", "stock", "deposit"]:
            series = series_map[series_id]
            if series.get("render_type") != "bar":
                raise SystemExit(f"household-balance-sheet {series_id} must render as bar")
            if series["y_axis_id"] != "left":
                raise SystemExit(f"household-balance-sheet {series_id} must use the left axis")
            if series.get("show_point_markers") not in {None, False}:
                raise SystemExit(f"household-balance-sheet {series_id} bars must not show point markers")
            if not series.get("stack"):
                raise SystemExit(f"household-balance-sheet {series_id} must declare a stack")
            if bar_stack is None:
                bar_stack = series["stack"]
            elif series["stack"] != bar_stack:
                raise SystemExit("household-balance-sheet bar series must share the same stack")

        total_series = series_map["total"]
        if total_series.get("render_type") != "line":
            raise SystemExit("household-balance-sheet total must render as line")
        if total_series["y_axis_id"] != "left":
            raise SystemExit("household-balance-sheet total must use the left axis")
        if total_series.get("show_point_markers") is not True:
            raise SystemExit("household-balance-sheet total must enable point markers")

        housing_values = series_map["housing"]["values"]
        stock_values = series_map["stock"]["values"]
        deposit_values = series_map["deposit"]["values"]
        total_values = total_series["values"]
        for housing_value, stock_value, deposit_value, total_value in zip(
            housing_values, stock_values, deposit_values, total_values, strict=True
        ):
            if any(value is None for value in (housing_value, stock_value, deposit_value)):
                if total_value is not None:
                    raise SystemExit("household-balance-sheet total must be null when any component is missing")
            elif total_value is None:
                raise SystemExit("household-balance-sheet total must exist when all three components are present")

    if chart_id == "inflation":
        for label in labels:
            datetime.strptime(label, "%Y-%m")
        datetime.strptime(chart["latest_time_text"], "%Y-%m")

        series_axis_map = {series["series_id"]: series["y_axis_id"] for series in chart["series"]}
        expected_axis_map = {
            "ppi-yoy": "left",
            "cpi-yoy": "right",
            "core-cpi-yoy": "right",
        }
        if series_axis_map != expected_axis_map:
            raise SystemExit("inflation chart must use ppi-yoy on left axis and cpi/core-cpi on right axis")
        series_gap_map = {series["series_id"]: series["span_gaps"] for series in chart["series"]}
        expected_gap_map = {
            "ppi-yoy": False,
            "cpi-yoy": True,
            "core-cpi-yoy": True,
        }
        if series_gap_map != expected_gap_map:
            raise SystemExit("inflation chart must keep CPI and core CPI continuous while PPI keeps original gaps")
        if chart.get("show_point_markers"):
            raise SystemExit("inflation chart must not enable point markers")
        if len(chart.get("latest_changes", [])) != 3:
            raise SystemExit("inflation chart must expose 3 latest_changes items")
        if {item["basis_label"] for item in chart["latest_changes"]} != {"环比"}:
            raise SystemExit("inflation chart latest_changes must all use 环比")

    if chart_id == "__legacy_income__":
        for label in labels:
            datetime.strptime(label, "%Y")
        datetime.strptime(chart["latest_time_text"], "%Y")
        expected_ids = {
            "listed-company-salary-yoy",
            "urban-disposable-income-yoy",
        }
        actual_ids = {series["series_id"] for series in chart["series"]}
        if actual_ids != expected_ids:
            raise SystemExit("income chart must expose salary and urban disposable income growth lines")
        if any(series["y_axis_id"] != "left" for series in chart["series"]):
            raise SystemExit("income chart must use the left axis")
        if any(series["span_gaps"] for series in chart["series"]):
            raise SystemExit("income chart must not force span_gaps")
        if chart.get("show_point_markers"):
            raise SystemExit("income chart must not enable point markers")
        if len(chart.get("latest_changes", [])) != 2:
            raise SystemExit("income chart must expose 2 latest_changes items")
        if {item["basis_label"] for item in chart["latest_changes"]} != {"环比"}:
            raise SystemExit("income chart latest_changes must all use 环比")

    if chart_id == "income":
        quarter_keys = [parse_quarter_label(label) for label in labels]
        if quarter_keys[0] != (2017, 1):
            raise SystemExit("income chart must start from 2017Q1")

        expected_quarter_keys = []
        year, quarter = quarter_keys[0]
        while (year, quarter) <= quarter_keys[-1]:
            expected_quarter_keys.append((year, quarter))
            if quarter == 4:
                year += 1
                quarter = 1
            else:
                quarter += 1

        if quarter_keys != expected_quarter_keys:
            raise SystemExit("income chart quarter labels must be continuous from 2017Q1")

        parse_quarter_label(chart["latest_time_text"])
        expected_ids = {
            "listed-company-salary-yoy",
            "urban-disposable-income-yoy",
        }
        series_map = {series["series_id"]: series for series in chart["series"]}
        if set(series_map) != expected_ids:
            raise SystemExit("income chart must expose salary and urban disposable income growth lines")
        if any(series["y_axis_id"] != "left" for series in chart["series"]):
            raise SystemExit("income chart must use the left axis")
        if chart.get("show_point_markers"):
            raise SystemExit("income chart must not enable chart-level point markers")
        if len(chart.get("latest_changes", [])) != 2:
            raise SystemExit("income chart must expose 2 latest_changes items")

        change_map = {item["label"]: item["basis_label"] for item in chart["latest_changes"]}
        if change_map != {
            "上市公司人均薪酬增速": "较上年",
            "城镇居民人均可支配收入增速": "较上季",
        }:
            raise SystemExit("income chart latest_changes must use 较上年 for salary and 较上季 for urban income")

        salary_series = series_map["listed-company-salary-yoy"]
        urban_series = series_map["urban-disposable-income-yoy"]
        if salary_series["span_gaps"] is not True:
            raise SystemExit("income chart salary series must enable span_gaps")
        if urban_series["span_gaps"] is not False:
            raise SystemExit("income chart urban income series must not enable span_gaps")
        if salary_series.get("show_point_markers") is not True:
            raise SystemExit("income chart salary series must enable show_point_markers")
        if urban_series.get("show_point_markers") is not True:
            raise SystemExit("income chart urban income series must enable show_point_markers")

        for (_, quarter), value in zip(quarter_keys, salary_series["values"], strict=True):
            if quarter != 4 and value is not None:
                raise SystemExit("income chart salary series must only place values on Q4 labels")

        if all(value is None for value in urban_series["values"]):
            raise SystemExit("income chart urban income series must contain quarterly values")

    if chart_id == "depositor-intentions":
        for label in labels:
            datetime.strptime(label.replace("Q1", "-03-31").replace("Q2", "-06-30").replace("Q3", "-09-30").replace("Q4", "-12-31"), "%Y-%m-%d")
        datetime.strptime(
            chart["latest_time_text"].replace("Q1", "-03-31").replace("Q2", "-06-30").replace("Q3", "-09-30").replace("Q4", "-12-31"),
            "%Y-%m-%d",
        )
        expected_ids = {"more-consumption", "more-investment", "more-savings"}
        actual_ids = {series["series_id"] for series in chart["series"]}
        if actual_ids != expected_ids:
            raise SystemExit("depositor-intentions chart must expose consumption, investment and savings intentions")
        if any(series["y_axis_id"] != "left" for series in chart["series"]):
            raise SystemExit("depositor-intentions chart must use the left axis only")
        if chart.get("show_point_markers"):
            raise SystemExit("depositor-intentions chart must not enable point markers")
        if len(chart.get("latest_changes", [])) != 3:
            raise SystemExit("depositor-intentions chart must expose 3 latest_changes items")
        if {item["basis_label"] for item in chart["latest_changes"]} != {"环比"}:
            raise SystemExit("depositor-intentions chart latest_changes must all use 环比")

    if chart_id == "urban-apc-seasonal":
        if labels != ["Q1", "Q2", "Q3", "Q4"]:
            raise SystemExit("urban-apc-seasonal chart labels must be Q1/Q2/Q3/Q4")
        datetime.strptime(
            chart["latest_time_text"].replace("Q1", "-03-31").replace("Q2", "-06-30").replace("Q3", "-09-30").replace("Q4", "-12-31"),
            "%Y-%m-%d",
        )
        if any(series["y_axis_id"] != "left" for series in chart["series"]):
            raise SystemExit("urban-apc-seasonal chart must use the left axis only")
        for series in chart["series"]:
            if len(series["label"]) != 4 or not series["label"].isdigit():
                raise SystemExit("urban-apc-seasonal chart labels each line by year")
        if chart.get("show_point_markers") is not True:
            raise SystemExit("urban-apc-seasonal chart must enable point markers")
        if len(chart.get("latest_changes", [])) != 1:
            raise SystemExit("urban-apc-seasonal chart must expose exactly 1 latest_changes item")
        if chart["latest_changes"][0]["basis_label"] != "同比":
            raise SystemExit("urban-apc-seasonal chart latest_changes must use 同比")

    if chart_id == "consumer-confidence":
        for label in labels:
            datetime.strptime(label, "%Y-%m")
        datetime.strptime(chart["latest_time_text"], "%Y-%m")
        expected_ids = {
            "consumer-confidence",
            "employment-confidence",
            "income-confidence",
            "consumption-willingness",
        }
        actual_ids = {series["series_id"] for series in chart["series"]}
        if actual_ids != expected_ids:
            raise SystemExit("consumer-confidence chart must expose confidence, employment, income and consumption lines")
        if any(series["y_axis_id"] != "left" for series in chart["series"]):
            raise SystemExit("consumer-confidence chart must use the left axis only")
        if chart.get("show_point_markers"):
            raise SystemExit("consumer-confidence chart must not enable point markers")
        if len(chart.get("latest_changes", [])) != 4:
            raise SystemExit("consumer-confidence chart must expose 4 latest_changes items")
        if {item["basis_label"] for item in chart["latest_changes"]} != {"环比"}:
            raise SystemExit("consumer-confidence chart latest_changes must all use 环比")

    if chart_id == "consumer-broker-share":
        for label in labels:
            datetime.strptime(label, "%Y-%m")
        datetime.strptime(chart["latest_time_text"], "%Y-%m")
        expected_ids = {"consumer-recommendation-share"}
        actual_ids = {series["series_id"] for series in chart["series"]}
        if actual_ids != expected_ids:
            raise SystemExit("consumer-broker-share chart must expose the consumer-recommendation-share line")
        if any(series["y_axis_id"] != "left" for series in chart["series"]):
            raise SystemExit("consumer-broker-share chart must use the left axis only")
        if any(series["span_gaps"] for series in chart["series"]):
            raise SystemExit("consumer-broker-share chart must not force span_gaps")
        if chart.get("show_point_markers"):
            raise SystemExit("consumer-broker-share chart must not enable point markers")


def validate_market_scatter_chart(chart: dict, section_id: str) -> None:
    chart_id = chart["chart_id"]
    if chart["chart_type"] != "scatter":
        raise SystemExit(f"{section_id} / {chart_id} chart_type must be scatter")
    if chart["x_axis_label"] != "PB五年分位数":
        raise SystemExit(f"{section_id} / {chart_id} x_axis_label must be PB五年分位数")
    if chart["y_axis_label"] != "ROE五年分位数":
        raise SystemExit(f"{section_id} / {chart_id} y_axis_label must be ROE五年分位数")
    if chart["latest_label"] != "数据截至":
        raise SystemExit(f"{section_id} / {chart_id} latest_label must be 数据截至")
    latest_time_text = chart["latest_time_text"]
    match = latest_time_text.split(" / ")
    if len(match) != 2 or not match[0].startswith("PB ") or not match[1].startswith("ROE "):
        raise SystemExit(f"{section_id} / {chart_id} latest_time_text must be formatted as PB YYYY-MM-DD / ROE YYYY-MM-DD")
    datetime.strptime(match[0][3:], "%Y-%m-%d")
    datetime.strptime(match[1][4:], "%Y-%m-%d")

    expected_level = "L1" if chart_id == "consumer-pb-roe-l1" else "L2"
    expected_max_count = (
        EXPECTED_SW2021_L1_COUNT
        if expected_level == "L1"
        else EXPECTED_CONSUMER_CROWDING_L2_COUNT
    )
    expected_max_consumer = (
        EXPECTED_CONSUMER_CROWDING_L1_COUNT
        if expected_level == "L1"
        else EXPECTED_CONSUMER_CROWDING_L2_COUNT
    )
    points = chart["points"]
    if not 1 <= len(points) <= expected_max_count:
        raise SystemExit(f"{section_id} / {chart_id} point count must be between 1 and {expected_max_count}")

    seen_codes = set()
    consumer_count = 0
    for point in points:
        if point["ts_code"] in seen_codes:
            raise SystemExit(f"{section_id} / {chart_id} contains duplicate ts_code: {point['ts_code']}")
        seen_codes.add(point["ts_code"])
        if point["level"] != expected_level:
            raise SystemExit(f"{section_id} / {chart_id} points must all have level {expected_level}")
        if not 0 <= point["pb_percentile_5y"] <= 100:
            raise SystemExit(f"{section_id} / {chart_id} pb_percentile_5y must be within 0-100")
        if not 0 <= point["roe_percentile_5y"] <= 100:
            raise SystemExit(f"{section_id} / {chart_id} roe_percentile_5y must be within 0-100")
        datetime.strptime(point["latest_pb_date"], "%Y-%m-%d")
        datetime.strptime(point["latest_roe_period"], "%Y-%m-%d")
        if expected_level == "L1":
            if point["parent_l1_name"] not in (None, ""):
                raise SystemExit(f"{section_id} / {chart_id} L1 points must not carry parent_l1_name")
        else:
            if not point["parent_l1_name"]:
                raise SystemExit(f"{section_id} / {chart_id} L2 points must include parent_l1_name")
        if point["is_consumer_related"]:
            consumer_count += 1

    if consumer_count < 1 or consumer_count > expected_max_consumer:
        raise SystemExit(f"{section_id} / {chart_id} highlighted consumer point count must be between 1 and {expected_max_consumer}")
    if chart_id == "consumer-pb-roe-l2" and consumer_count != len(points):
        raise SystemExit("consumer-pb-roe-l2 must only include consumer-related L2 industries")


def validate_macro(macro_payload: dict) -> None:
    actual_section_ids = [section["section_id"] for section in macro_payload["sections"]]
    if actual_section_ids != EXPECTED_MACRO_SECTION_IDS:
        raise SystemExit("Macro section order does not match the expected board layout")

    seen_ids = set()
    for section in macro_payload["sections"]:
        section_id = section["section_id"]
        if section_id in seen_ids:
            raise SystemExit(f"Duplicate macro section: {section_id}")
        seen_ids.add(section_id)

        status = section["status"]
        chart = section.get("chart")
        charts = section.get("charts")
        if status == "ready":
            if bool(chart) == bool(charts):
                raise SystemExit(f"{section_id} ready section must include exactly one of chart or charts")
            if chart is not None:
                validate_macro_chart(chart, section_id)
            if charts is not None:
                seen_chart_ids = set()
                for item in charts:
                    chart_id = item["chart_id"]
                    if chart_id in seen_chart_ids:
                        raise SystemExit(f"{section_id} charts contain duplicate chart_id: {chart_id}")
                    seen_chart_ids.add(chart_id)
                    validate_macro_chart(item, section_id)
        elif status == "coming_soon":
            if chart is not None or charts is not None:
                raise SystemExit(f"{section_id} coming_soon section must not include chart data")
        elif status == "error":
            if chart is not None or charts is not None:
                raise SystemExit(f"{section_id} error section must not include chart data")
        else:
            raise SystemExit(f"Unexpected macro section status: {status}")


def validate_real_estate_high_frequency(board_payload: dict) -> None:
    actual_section_ids = [section["section_id"] for section in board_payload["sections"]]
    if actual_section_ids != EXPECTED_REAL_ESTATE_HIGH_FREQUENCY_SECTION_IDS:
        raise SystemExit("Real-estate-high-frequency section order does not match the expected board layout")

    expected_chart_ids = {
        "second-hand-housing": {"second-hand-price-mom", "second-hand-activity"},
        "new-home": {"new-home-sales-monthly", "new-home-price-tier"},
    }

    for section in board_payload["sections"]:
        section_id = section["section_id"]
        status = section["status"]
        chart = section.get("chart")
        charts = section.get("charts")

        if status == "ready":
            if chart is not None:
                raise SystemExit(f"{section_id} ready section must not use single chart payload")
            if not charts:
                raise SystemExit(f"{section_id} ready section must include charts")
            seen_chart_ids = set()
            for item in charts:
                chart_id = item["chart_id"]
                if chart_id in seen_chart_ids:
                    raise SystemExit(f"{section_id} charts contain duplicate chart_id: {chart_id}")
                if chart_id not in expected_chart_ids[section_id]:
                    raise SystemExit(f"{section_id} contains unexpected chart_id: {chart_id}")
                seen_chart_ids.add(chart_id)
                validate_macro_chart(item, section_id)

                if chart_id in {"second-hand-price-mom", "second-hand-activity"}:
                    label_dates = [datetime.strptime(label, "%Y-%m-%d") for label in item["labels"]]
                    latest_date = datetime.strptime(item["latest_time_text"], "%Y-%m-%d")
                    if len(item["series"]) != 5:
                        raise SystemExit(f"{section_id} / {chart_id} must contain 5 tier series")
                    if label_dates[-1] != latest_date:
                        raise SystemExit(f"{section_id} / {chart_id} latest_time_text must match the last label")
                    if label_dates[0] < latest_date - timedelta(days=REAL_ESTATE_HIGH_FREQUENCY_ROLLING_YEARS * 366 + 10):
                        raise SystemExit(f"{section_id} / {chart_id} must only keep the latest rolling 3 years of weekly data")
                elif chart_id in {"new-home-sales-monthly", "new-home-price-tier"}:
                    label_dates = [datetime.strptime(label, "%Y-%m") for label in item["labels"]]
                    latest_date = datetime.strptime(item["latest_time_text"], "%Y-%m")
                    datetime.strptime(item["latest_time_text"], "%Y-%m")
                    expected_series_count = 2
                    if len(item["series"]) != expected_series_count:
                        raise SystemExit(
                            f"{section_id} / {chart_id} must contain {expected_series_count} series"
                        )
                    if label_dates[-1] != latest_date:
                        raise SystemExit(f"{section_id} / {chart_id} latest_time_text must match the last label")
                    if label_dates[0] < latest_date - timedelta(days=REAL_ESTATE_HIGH_FREQUENCY_NEW_HOME_ROLLING_YEARS * 366 + 31):
                        raise SystemExit(f"{section_id} / {chart_id} must only keep the latest rolling 5 years of monthly data")
                    if chart_id == "new-home-price-tier":
                        if item["chart_type"] != "combo":
                            raise SystemExit(f"{section_id} / {chart_id} must use combo chart_type")
                        series_map = {series["series_id"]: series for series in item["series"]}
                        if set(series_map) != {"price_yoy", "price_mom"}:
                            raise SystemExit(f"{section_id} / {chart_id} must expose price_yoy and price_mom")
                        if series_map["price_yoy"].get("render_type") != "line":
                            raise SystemExit(f"{section_id} / {chart_id} price_yoy must render as line")
                        if series_map["price_yoy"].get("show_point_markers") is not True:
                            raise SystemExit(f"{section_id} / {chart_id} price_yoy must enable point markers")
                        if series_map["price_mom"].get("render_type") != "bar":
                            raise SystemExit(f"{section_id} / {chart_id} price_mom must render as bar")
                        if series_map["price_mom"].get("show_point_markers") not in {None, False}:
                            raise SystemExit(f"{section_id} / {chart_id} price_mom must not enable point markers")
                    else:
                        if item["chart_type"] != "line":
                            raise SystemExit(f"{section_id} / {chart_id} must use line chart_type")
                        series_map = {series["series_id"]: series for series in item["series"]}
                        if set(series_map) != {"sales_area_yoy", "sales_amount_yoy"}:
                            raise SystemExit(f"{section_id} / {chart_id} must expose sales_area_yoy and sales_amount_yoy")
                        if any(series["span_gaps"] is not True for series in series_map.values()):
                            raise SystemExit(f"{section_id} / {chart_id} must span gaps so January breakpoints stay connected")

            if len(seen_chart_ids) < len(expected_chart_ids[section_id]) and not section.get("placeholder_message"):
                raise SystemExit(f"{section_id} partial ready section must include placeholder_message")
        elif status == "coming_soon":
            if chart is not None or charts is not None:
                raise SystemExit(f"{section_id} coming_soon section must not include chart data")
        elif status == "error":
            if chart is not None or charts is not None:
                raise SystemExit(f"{section_id} error section must not include chart data")
        else:
            raise SystemExit(f"Unexpected real-estate-high-frequency section status: {status}")


def validate_market(market_payload: dict) -> None:
    actual_section_ids = [section["section_id"] for section in market_payload["sections"]]
    if actual_section_ids != EXPECTED_MARKET_SECTION_IDS:
        raise SystemExit("Market tracking section order does not match the expected board layout")

    seen_ids = set()
    for section in market_payload["sections"]:
        section_id = section["section_id"]
        if section_id in seen_ids:
            raise SystemExit(f"Duplicate market section: {section_id}")
        seen_ids.add(section_id)

        status = section["status"]
        chart = section.get("chart")
        charts = section.get("charts")
        coverage = section.get("coverage")
        notice_message = section.get("notice_message")
        recommended_stocks = section.get("recommended_stocks", [])
        crowdedness_groups = section.get("crowdedness_groups", [])

        if status == "ready":
            if section_id == "consumer-pb-roe":
                if chart is not None or coverage is not None or recommended_stocks or crowdedness_groups:
                    raise SystemExit("consumer-pb-roe ready section must only include scatter charts and optional notice_message")
                if not charts or not isinstance(charts, list):
                    raise SystemExit("consumer-pb-roe ready section must include charts")
                if len(charts) > 2:
                    raise SystemExit("consumer-pb-roe ready section must not include more than 2 charts")
                seen_chart_ids = set()
                for item in charts:
                    chart_id = item["chart_id"]
                    if chart_id in seen_chart_ids:
                        raise SystemExit(f"consumer-pb-roe charts contain duplicate chart_id: {chart_id}")
                    if chart_id not in {"consumer-pb-roe-l1", "consumer-pb-roe-l2"}:
                        raise SystemExit(f"Unexpected consumer-pb-roe chart_id: {chart_id}")
                    seen_chart_ids.add(chart_id)
                    validate_market_scatter_chart(item, section_id)
                if len(charts) < 2 and not notice_message:
                    raise SystemExit("consumer-pb-roe partial ready section must include notice_message")
            elif section_id == "consumer-crowdedness":
                if chart is not None or coverage is not None or recommended_stocks:
                    raise SystemExit("consumer-crowdedness ready section must not include chart, coverage or recommended_stocks")
                if charts:
                    raise SystemExit("consumer-crowdedness ready section must not include charts")
                if len(crowdedness_groups) != 2:
                    raise SystemExit("consumer-crowdedness must include exactly 2 crowdedness_groups")
                group_map = {group["group_id"]: group for group in crowdedness_groups}
                if set(group_map) != {"l1", "l2"}:
                    raise SystemExit("consumer-crowdedness crowdedness_groups must contain l1 and l2 exactly once")

                group_dates = set()
                for group_id, expected_title in (("l1", "申万一级行业"), ("l2", "申万二级行业")):
                    group = group_map[group_id]
                    if group["title"] != expected_title:
                        raise SystemExit(f"consumer-crowdedness {group_id} title must be {expected_title}")
                    datetime.strptime(group["as_of_date"], "%Y-%m-%d")
                    group_dates.add(group["as_of_date"])

                    industries = group["industries"]
                    actual_count = len(industries)
                    expected_level = "L1" if group_id == "l1" else "L2"
                    expected_count = (
                        EXPECTED_CONSUMER_CROWDING_L1_COUNT if group_id == "l1" else EXPECTED_CONSUMER_CROWDING_L2_COUNT
                    )
                    if actual_count > expected_count:
                        raise SystemExit(f"consumer-crowdedness {group_id} must contain no more than {expected_count} industries")

                    seen_codes = set()
                    l1_names = set()
                    previous_sort_key = None
                    for item in industries:
                        if item["level"] != expected_level:
                            raise SystemExit(f"{item['ts_code']} level must be {expected_level} in {group_id}")
                        if item["ts_code"] in seen_codes:
                            raise SystemExit(f"Duplicate crowdedness ts_code in {group_id}: {item['ts_code']}")
                        seen_codes.add(item["ts_code"])

                        score = float(item["total_score"])
                        if not score.is_integer() or not 0 <= score <= 4:
                            raise SystemExit(f"{item['ts_code']} total_score must be an integer between 0 and 4")
                        if not 0 <= item["crowded_20d_count"] <= 20:
                            raise SystemExit(f"{item['ts_code']} crowded_20d_count must be between 0 and 20")

                        if group_id == "l1":
                            if item["parent_l1_name"] not in (None, ""):
                                raise SystemExit(f"{item['ts_code']} parent_l1_name must be empty in l1 group")
                            l1_names.add(item["industry_name"])
                        else:
                            if not item["parent_l1_name"]:
                                raise SystemExit(f"{item['ts_code']} parent_l1_name must be set in l2 group")
                            if item["parent_l1_name"] not in EXPECTED_CONSUMER_CROWDING_L1_NAMES:
                                raise SystemExit(f"{item['ts_code']} parent_l1_name is outside the allowed consumer L1 set")

                        sort_key = (
                            -int(item["high_crowding"]),
                            -int(item["excluded_signal"]),
                            -int(score),
                            item["industry_name"],
                        )
                        if previous_sort_key is not None and sort_key < previous_sort_key:
                            raise SystemExit("consumer-crowdedness industries must be sorted by warning, excluded_signal, total_score, industry_name")
                        previous_sort_key = sort_key

                    if group_id == "l1" and not l1_names.issubset(EXPECTED_CONSUMER_CROWDING_L1_NAMES):
                        raise SystemExit("consumer-crowdedness l1 industry set must stay within the expected consumer L1 universe")

                if len(group_dates) != 1:
                    raise SystemExit("consumer-crowdedness l1 and l2 groups must share the same as_of_date")
                total_crowdedness_count = sum(len(group["industries"]) for group in crowdedness_groups)
                if total_crowdedness_count < 1:
                    raise SystemExit("consumer-crowdedness ready section must contain at least 1 industry")
                has_partial_crowdedness = any(
                    len(group["industries"])
                    < (
                        EXPECTED_CONSUMER_CROWDING_L1_COUNT if group["group_id"] == "l1" else EXPECTED_CONSUMER_CROWDING_L2_COUNT
                    )
                    for group in crowdedness_groups
                )
                if has_partial_crowdedness and not notice_message:
                    raise SystemExit("consumer-crowdedness partial ready section must include notice_message")
            else:
                if charts:
                    raise SystemExit(f"{section_id} should not include charts")
                if chart is None:
                    raise SystemExit(f"{section_id} ready market section must include a chart")
                validate_macro_chart(chart, section_id)
                if crowdedness_groups:
                    raise SystemExit(f"{section_id} should not include crowdedness_groups")

            if section_id == "consumer-broker-share":
                if coverage is None:
                    raise SystemExit("consumer-broker-share ready section must include coverage")
                datetime.strptime(coverage["latest_month"], "%Y-%m")
                if coverage["latest_month"] != chart["latest_time_text"]:
                    raise SystemExit("consumer-broker-share coverage latest_month must match chart latest_time_text")
                if coverage["months_covered"] < 1:
                    raise SystemExit("consumer-broker-share months_covered must be >= 1")
                if coverage["latest_broker_count"] < 1:
                    raise SystemExit("consumer-broker-share latest_broker_count must be >= 1")
                if coverage["min_broker_count"] > coverage["max_broker_count"]:
                    raise SystemExit("consumer-broker-share min_broker_count must be <= max_broker_count")
                ensure_percentile(coverage["latest_consumer_share_pct"], "consumer-broker-share latest_consumer_share_pct")
                if coverage["latest_consumer_stock_count"] != len(recommended_stocks):
                    raise SystemExit("consumer-broker-share latest_consumer_stock_count must match recommended_stocks length")
                if not coverage["comparison_note"]:
                    raise SystemExit("consumer-broker-share coverage comparison_note must not be empty")

                previous_broker_count = None
                for item in recommended_stocks:
                    if item["broker_count"] != len(item["broker_names"]):
                        raise SystemExit(f"{item['ts_code']} broker_count must match broker_names length")
                    if len(set(item["broker_names"])) != len(item["broker_names"]):
                        raise SystemExit(f"{item['ts_code']} broker_names must be unique")
                    if "previous_broker_count" in item and item["previous_broker_count"] is not None and item["previous_broker_count"] < 0:
                        raise SystemExit(f"{item['ts_code']} previous_broker_count must be >= 0")
                    if "broker_count_delta" in item and item["broker_count_delta"] is not None:
                        previous_count = item.get("previous_broker_count") or 0
                        if item["broker_count_delta"] != item["broker_count"] - previous_count:
                            raise SystemExit(f"{item['ts_code']} broker_count_delta must equal broker_count - previous_broker_count")
                    if previous_broker_count is not None and item["broker_count"] > previous_broker_count:
                        raise SystemExit("recommended_stocks must be sorted by broker_count descending")
                    previous_broker_count = item["broker_count"]
        elif status == "coming_soon":
            if chart is not None or charts is not None or coverage is not None or recommended_stocks or crowdedness_groups:
                raise SystemExit(f"{section_id} coming_soon market section must not include chart or stock data")
        elif status == "error":
            if chart is not None or charts is not None or coverage is not None or recommended_stocks or crowdedness_groups:
                raise SystemExit(f"{section_id} error market section must not include chart or stock data")
        else:
            raise SystemExit(f"Unexpected market section status: {status}")


def main() -> None:
    config = load_json(CONFIG_PATH)
    panels = validate_schema("sector-market-panels.json", "sector-market-panels.schema.json")
    series_payload = validate_schema("sector-market-series.json", "sector-market-series.schema.json")
    macro_payload = validate_schema("macro-board.json", "macro-board.schema.json")
    real_estate_high_frequency_payload = validate_schema(
        "real-estate-high-frequency-board.json",
        "real-estate-high-frequency-board.schema.json",
    )
    market_payload = validate_schema("market-tracking-board.json", "market-tracking-board.schema.json")

    validate_common(panels, "sector-market-panels.json")
    validate_common(series_payload, "sector-market-series.json")
    validate_common(macro_payload, "macro-board.json")
    validate_common(real_estate_high_frequency_payload, "real-estate-high-frequency-board.json")
    validate_common(market_payload, "market-tracking-board.json")

    valid_keys = validate_panels(panels, config)
    validate_series(series_payload, valid_keys)
    validate_macro(macro_payload)
    validate_real_estate_high_frequency(real_estate_high_frequency_payload)
    validate_market(market_payload)

    print(
        "Sector market JSON, macro board JSON, real-estate-high-frequency board JSON "
        "and market tracking JSON passed schema and semantic validation."
    )


if __name__ == "__main__":
    main()
