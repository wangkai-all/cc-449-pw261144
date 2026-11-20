"""Versioned response parsers.

Upstream responses are JSON objects. Each parser version normalizes them
into a fixed field set. v2 is an *upgrade* of v1: it normalizes types and
adds derived fields, so re-parsing an old snapshot with v2 yields a
meaningful field diff.
"""
from __future__ import annotations

import json
import re

from .errors import ValidationError


class ParseError(Exception):
    """Payload cannot be parsed by this parser version."""


CURRENCY_SYMBOLS = {"¥": "CNY", "$": "USD", "€": "EUR", "£": "GBP"}

_PRICE_RE = re.compile(r"^\s*([^\d\s]?)\s*(\d+(?:\.\d+)?)\s*$")


def _load(payload: str) -> dict:
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ParseError(f"payload is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ParseError("payload JSON must be an object")
    return data


def _str_field(data: dict, key: str):
    value = data.get(key)
    if value is None:
        return None
    return str(value).strip()


def parse_v1(payload: str) -> dict:
    """v1: flat extraction, everything stays a string, minimal normalization."""
    data = _load(payload)
    tags_raw = _str_field(data, "tags")
    return {
        "title": _str_field(data, "title"),
        "price": _str_field(data, "price"),
        "tags": None if tags_raw is None else tags_raw.split(","),
        "sku": _str_field(data, "sku"),
        "stock": _str_field(data, "stock"),
    }


def parse_v2(payload: str) -> dict:
    """v2: typed fields, normalized tags, derived currency / in_stock."""
    data = _load(payload)
    base = parse_v1(payload)

    title = base["title"]
    if title is not None:
        title = re.sub(r"\s+", " ", title)

    price_value = None
    currency = None
    if base["price"] is not None:
        match = _PRICE_RE.match(base["price"])
        if not match:
            raise ParseError(f"unparseable price: {base['price']!r}")
        symbol, number = match.groups()
        price_value = float(number)
        currency = CURRENCY_SYMBOLS.get(symbol, symbol or None)

    tags = base["tags"]
    if tags is not None:
        tags = sorted({t.strip().lower() for t in tags if t.strip()})

    stock = None
    if base["stock"] is not None:
        try:
            stock = int(base["stock"])
        except ValueError as exc:
            raise ParseError(f"unparseable stock: {base['stock']!r}") from exc

    sku = base["sku"]
    if sku is not None:
        sku = sku.upper()

    return {
        "title": title,
        "price_value": price_value,
        "currency": currency,
        "tags": tags,
        "sku": sku,
        "stock": stock,
        "in_stock": None if stock is None else stock > 0,
    }


PARSER_VERSIONS = ["v1", "v2"]  # ordered oldest -> newest
PARSERS = {"v1": parse_v1, "v2": parse_v2}
DEFAULT_VERSION = "v1"

PARSER_FIELDS = {
    "v1": ["title", "price", "tags", "sku", "stock"],
    "v2": ["title", "price_value", "currency", "tags", "sku", "stock", "in_stock"],
}


def ensure_version(version: str) -> None:
    if version not in PARSERS:
        raise ValidationError(
            f"unknown parser version {version!r}; available: {', '.join(PARSER_VERSIONS)}"
        )


def is_upgrade(from_version: str, to_version: str) -> bool:
    ensure_version(from_version)
    ensure_version(to_version)
    return PARSER_VERSIONS.index(to_version) > PARSER_VERSIONS.index(from_version)


def parse(version: str, payload: str) -> dict:
    ensure_version(version)
    return PARSERS[version](payload)
