_TRUTHY = {"1", "yes", "on", "true"}
_FALSY = {"0", "no", "off", "false"}


def celsius_to_fahrenheit(c: float) -> float:
    return c * 9 / 5 + 32


def parse_bool(value) -> float:
    s = str(value).strip().lower()
    if s in _TRUTHY:
        return 1.0
    if s in _FALSY:
        return 0.0
    raise ValueError(f"Cannot parse boolean: {value!r}")


def make_metric_key(name: str, labels: dict) -> str:
    if not labels:
        return name
    label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return f"{name}{{{label_str}}}"
