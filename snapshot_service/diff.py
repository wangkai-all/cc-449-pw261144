"""Field-level diff between two parse results."""
from __future__ import annotations


def diff_fields(old, new, prefix: str = "") -> list[dict]:
    """Return a list of {field, change, old, new} entries.

    Dicts are compared recursively with dot-path field names; any other
    values are compared by equality. A subtree that appears or disappears
    wholesale is reported as a single added/removed entry.
    """
    changes: list[dict] = []
    if isinstance(old, dict) and isinstance(new, dict):
        for key in sorted(old.keys() | new.keys()):
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in old:
                changes.append({"field": path, "change": "added", "old": None, "new": new[key]})
            elif key not in new:
                changes.append({"field": path, "change": "removed", "old": old[key], "new": None})
            else:
                changes.extend(diff_fields(old[key], new[key], path))
    elif old != new:
        changes.append({"field": prefix, "change": "changed", "old": old, "new": new})
    return changes
