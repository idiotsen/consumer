from __future__ import annotations

import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
PANELS_PATH = ROOT / "public" / "data" / "sector-market-panels.json"


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def check_url(url: str) -> tuple[bool, str]:
    request = Request(url, method="HEAD", headers={"User-Agent": "macro-dashboard-checker/2.0"})
    try:
        with urlopen(request, timeout=10) as response:
            return True, str(response.status)
    except HTTPError as error:
        return False, f"HTTP {error.code}"
    except URLError as error:
        return False, str(error.reason)


def iter_urls() -> list[str]:
    if not PANELS_PATH.exists():
        return []

    panels = load_json(PANELS_PATH)
    urls: list[str] = []
    for board in panels.get("boards", []):
        for industry in board.get("industries", []):
            for field in ("link", "source_url"):
                value = industry.get(field)
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    urls.append(value)
    return urls


def main() -> None:
    urls = iter_urls()
    if not urls:
        print("No external links configured in sector-market-panels.json.")
        return

    failed = []
    for url in urls:
        ok, detail = check_url(url)
        status = "OK" if ok else "FAIL"
        print(f"[{status}] {url} -> {detail}")
        if not ok:
            failed.append(url)

    if failed:
        raise SystemExit(f"Found {len(failed)} external link(s) that failed validation.")


if __name__ == "__main__":
    main()
