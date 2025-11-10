thonfrom typing import Any, Dict, List

def _normalize(values: List[float]) -> List[float]:
    total = sum(values)
    if total <= 0:
        return [1.0 / len(values)] * len(values) if values else []
    return [v / total for v in values]

def parse_demographics(raw_demographics: Dict[str, Any]) -> Dict[str, Any]:
    age_distribution_raw = raw_demographics.get("ageDistribution") or []
    gender_distribution_raw = raw_demographics.get("genderDistribution") or {}

    ages: List[Dict[str, Any]] = []
    for item in age_distribution_raw:
        try:
            min_age = int(item.get("minAge"))
            max_age = int(item.get("maxAge"))
            value = float(item.get("value", 0) or 0)
        except Exception:
            continue
        ages.append({"minAge": min_age, "maxAge": max_age, "value": value})

    ages.sort(key=lambda x: x["minAge"])
    values = [a["value"] for a in ages]
    if values:
        normalized = _normalize(values)
        for a, nv in zip(ages, normalized):
            a["value"] = round(nv, 3)

    try:
        male = float(gender_distribution_raw.get("male", 0) or 0)
        female = float(gender_distribution_raw.get("female", 0) or 0)
    except Exception:
        male = female = 0.0

    if male <= 0 and female <= 0:
        male = female = 0.5
    else:
        norm = _normalize([male, female])
        male, female = norm[0], norm[1]

    gender_distribution = {"male": round(male, 3), "female": round(female, 3)}

    return {
        "ageDistribution": ages,
        "genderDistribution": gender_distribution,
    }