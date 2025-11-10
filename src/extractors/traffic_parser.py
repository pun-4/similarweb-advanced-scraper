thonfrom typing import Any, Dict, List

def _ensure_sorted_historical(historical: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    try:
        return sorted(historical, key=lambda x: x.get("date", ""))
    except Exception:
        return historical

def _compute_growth(historical: List[Dict[str, Any]]) -> float:
    if len(historical) < 2:
        return 0.0
    first = float(historical[0].get("visits", 0) or 0)
    last = float(historical[-1].get("visits", 0) or 0)
    if first <= 0:
        return 0.0
    growth = (last - first) / first
    return round(growth, 3)

def _compute_average_visits(historical: List[Dict[str, Any]]) -> float:
    if not historical:
        return 0.0
    total = sum(float(item.get("visits", 0) or 0) for item in historical)
    return round(total / len(historical), 2)

def parse_traffic(
    traffic: Dict[str, Any],
    traffic_sources: Dict[str, Any],
    ranking: Dict[str, Any],
) -> Dict[str, Any]:
    historical = traffic.get("historical") or []
    if not isinstance(historical, list):
        historical = []

    historical_sorted = _ensure_sorted_historical(historical)
    growth = _compute_growth(historical_sorted)
    avg_visits = _compute_average_visits(historical_sorted)
    visits_total_count = traffic.get("visitsTotalCount")
    if visits_total_count is None:
        visits_total_count = sum(int(item.get("visits", 0) or 0) for item in historical_sorted)

    traffic_summary = {
        "historical": historical_sorted,
        "visitsTotalCount": visits_total_count,
        "averageMonthlyVisits": avg_visits,
        "visitsGrowthRate": growth,
    }

    return {
        "traffic": traffic_summary,
        "trafficSources": traffic_sources or {},
        "ranking": ranking or {},
    }