thonfrom typing import Any, Dict, List

def parse_competitors(raw_competitors: Dict[str, Any]) -> Dict[str, Any]:
    competitors_list = raw_competitors.get("topSimilarityCompetitors") or []
    cleaned: List[Dict[str, Any]] = []

    for comp in competitors_list:
        domain = str(comp.get("domain", "")).strip()
        if not domain:
            continue
        visits = comp.get("visitsTotalCount", 0) or 0
        similarity = comp.get("similarityScore", None)
        if similarity is None:
            similarity = 0.0

        cleaned.append(
            {
                "domain": domain,
                "visitsTotalCount": int(visits),
                "similarityScore": float(similarity),
            }
        )

    cleaned.sort(key=lambda c: (c.get("similarityScore", 0.0), c.get("visitsTotalCount", 0)), reverse=True)
    top_competitors = cleaned[:10]

    return {
        "topSimilarityCompetitors": top_competitors,
        "competitorsCount": len(cleaned),
    }