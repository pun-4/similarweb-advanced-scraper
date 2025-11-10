thonimport argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List

from extractors.traffic_parser import parse_traffic
from extractors.demographics_parser import parse_demographics
from extractors.competitors_parser import parse_competitors
from utils.logger import get_logger
from utils.retry_handler import retry
from src import synthetic_data  # type: ignore

# Fallback for when this module is executed directly (python src/main.py)
try:
    from . import synthetic_data as _synthetic_data  # type: ignore
    synthetic_data = _synthetic_data  # type: ignore
except Exception:  # pragma: no cover
    pass

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = Path(__file__).resolve().parent / "config"
DEFAULT_SETTINGS_PATH = CONFIG_DIR / "settings.json"

logger = get_logger("similarweb_scraper")

def load_settings(settings_path: Path = DEFAULT_SETTINGS_PATH) -> Dict[str, Any]:
    if not settings_path.exists():
        logger.warning("Settings file not found at %s, using built-in defaults.", settings_path)
        return {
            "input_file": "data/input_sample.json",
            "output_file": "data/output_example.json",
            "log_level": "INFO",
            "max_retries": 3,
            "retry_backoff_seconds": 1.5,
        }

    with settings_path.open("r", encoding="utf-8") as f:
        settings = json.load(f)

    log_level = settings.get("log_level", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)
    logging.getLogger().setLevel(level)
    logger.debug("Loaded settings: %s", settings)
    return settings

def resolve_path(path_str: str) -> Path:
    path = Path(path_str)
    if not path.is_absolute():
        path = BASE_DIR / path
    return path

def load_domains(input_path: Path) -> List[str]:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    suffix = input_path.suffix.lower()
    if suffix == ".json":
        with input_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and "domains" in data:
            domains = [str(d).strip() for d in data["domains"] if str(d).strip()]
        elif isinstance(data, list):
            # Either list of domains or list of objects with "domain"
            domains = []
            for item in data:
                if isinstance(item, str):
                    domains.append(item.strip())
                elif isinstance(item, dict) and "domain" in item:
                    domains.append(str(item["domain"]).strip())
        else:
            raise ValueError("Unsupported JSON structure for input domains.")
    elif suffix == ".csv":
        domains = []
        with input_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            if "domain" in fieldnames:
                for row in reader:
                    val = row.get("domain", "").strip()
                    if val:
                        domains.append(val)
            else:
                input_path.seek(0)
                reader_generic = csv.reader(f)
                for row in reader_generic:
                    if not row:
                        continue
                    val = str(row[0]).strip()
                    if val:
                        domains.append(val)
    elif suffix in {".txt", ".list"}:
        with input_path.open("r", encoding="utf-8") as f:
            domains = [line.strip() for line in f if line.strip()]
    else:
        raise ValueError(f"Unsupported input file type: {suffix}")

    unique_domains = sorted(set(domains))
    logger.info("Loaded %d domains (%d unique) from %s", len(domains), len(unique_domains), input_path)
    return unique_domains

@retry()
def fetch_domain_data(domain: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    Synthetic data generator that mimics a Similarweb-like payload.
    In a real deployment this function would perform authenticated HTTP
    requests to Similarweb or a proxy service.
    """
    logger.debug("Generating synthetic analytics for domain=%s", domain)
    payload = synthetic_data.generate_domain_payload(domain)
    logger.debug("Generated payload for %s: keys=%s", domain, list(payload.keys()))
    return payload

def build_record(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    domain = raw_payload.get("domain", "unknown")
    logger.debug("Building record for %s", domain)

    demographics = parse_demographics(raw_payload.get("demographics", {}))
    competitors = parse_competitors(raw_payload.get("competitors", {}))
    traffic_summary = parse_traffic(
        raw_payload.get("traffic", {}),
        raw_payload.get("trafficSources", {}),
        raw_payload.get("ranking", {}),
    )

    record: Dict[str, Any] = {
        "domain": domain,
        "overview": raw_payload.get("overview", {}),
        "interests": raw_payload.get("interests", []),
        "competitors": competitors,
        "searchesSource": raw_payload.get("searchesSource", {}),
        "incomingReferrals": raw_payload.get("incomingReferrals", {}),
        "adsSource": raw_payload.get("adsSource", {}),
        "socialNetworksSource": raw_payload.get("socialNetworksSource", {}),
        "technologies": raw_payload.get("technologies", []),
        "recentAds": raw_payload.get("recentAds", []),
        "demographics": demographics,
        "geography": raw_payload.get("geography", {}),
        "trafficSources": traffic_summary.get("trafficSources", {}),
        "ranking": traffic_summary.get("ranking", {}),
        "traffic": traffic_summary.get("traffic", {}),
    }
    logger.debug("Built record for %s", domain)
    return record

def save_output(records: Iterable[Dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = list(records)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info("Wrote %d records to %s", len(data), output_path)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Similarweb Advanced Scraper (synthetic demo pipeline)."
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Path to input domains file (overrides settings.json).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Path to output json file (overrides settings.json).",
    )
    parser.add_argument(
        "--settings",
        type=str,
        default=str(DEFAULT_SETTINGS_PATH),
        help="Path to settings.json (default: %(default)s)",
    )
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    settings = load_settings(Path(args.settings))

    input_file = resolve_path(args.input or settings.get("input_file", "data/input_sample.json"))
    output_file = resolve_path(args.output or settings.get("output_file", "data/output_example.json"))

    logger.info("Using input file: %s", input_file)
    logger.info("Using output file: %s", output_file)

    try:
        domains = load_domains(input_file)
    except Exception as exc:
        logger.error("Failed to load domains from %s: %s", input_file, exc)
        raise SystemExit(1) from exc

    results: List[Dict[str, Any]] = []
    for idx, domain in enumerate(domains, start=1):
        try:
            logger.info("[%d/%d] Processing %s", idx, len(domains), domain)
            payload = fetch_domain_data(domain, settings)
            record = build_record(payload)
            results.append(record)
        except Exception as exc:
            logger.exception("Failed to process domain %s: %s", domain, exc)

    save_output(results, output_file)

if __name__ == "__main__":
    main()