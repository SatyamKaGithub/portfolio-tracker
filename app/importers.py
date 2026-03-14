from __future__ import annotations

import base64
import math
import re
import zipfile
from dataclasses import dataclass
from io import BytesIO
from typing import Dict, Iterable, List, Optional
from xml.etree import ElementTree as ET

NS = {
    "main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}

HEADER_ALIASES = {
    "symbol": "symbol",
    "tradingsymbol": "symbol",
    "ticker": "symbol",
    "companyname": "company_name",
    "company": "company_name",
    "security": "company_name",
    "isin": "isin",
    "sector": "sector",
    "industry": "sector",
    "instrumenttype": "asset_type",
    "instrument": "asset_type",
    "assettype": "asset_type",
    "producttype": "asset_type",
    "quantity": "quantity",
    "qty": "quantity",
    "quantityavailable": "quantity",
    "availablequantity": "quantity",
    "quantitylongterm": "t1_quantity",
    "t1quantity": "t1_quantity",
    "averageprice": "avg_buy_cost",
    "averagecost": "avg_buy_cost",
    "avgprice": "avg_buy_cost",
    "avgbuycost": "avg_buy_cost",
    "buyprice": "avg_buy_cost",
    "investedvalue": "invested_amount",
    "investmentamount": "invested_amount",
    "investedamount": "invested_amount",
    "costvalue": "invested_amount",
    "prevclosingprice": "prev_close",
    "previousclosingprice": "prev_close",
    "prevclose": "prev_close",
    "previousclose": "prev_close",
    "ltp": "current_price",
    "currentprice": "current_price",
    "lastprice": "current_price",
    "currentvalue": "current_value",
    "daychange": "one_day_change",
    "onedaychange": "one_day_change",
    "pnl": "unrealized_pnl",
    "unrealizedpl": "unrealized_pnl",
    "unrealizedp&l": "unrealized_pnl",
    "unrealizedplpct": "unrealized_pnl_percent",
    "unrealizedpnlpct": "unrealized_pnl_percent",
    "unrealizep&lpct": "unrealized_pnl_percent",
    "unrealizedp&lpct": "unrealized_pnl_percent",
    "unrealisedpnl": "unrealized_pnl",
    "unrealizedpnl": "unrealized_pnl",
    "currency": "currency",
}

ESSENTIAL_HEADERS = {"symbol", "quantity", "avg_buy_cost"}


@dataclass
class ParsedWorkbook:
    rows: List[Dict[str, object]]
    sheet_name: str


def decode_base64_document(content_base64: str) -> bytes:
    try:
        return base64.b64decode(content_base64, validate=True)
    except Exception:
        if "," in content_base64:
            _, payload = content_base64.split(",", 1)
            return base64.b64decode(payload)
        raise ValueError("Invalid base64 file payload") from None


def parse_xlsx_holdings(file_bytes: bytes) -> ParsedWorkbook:
    try:
        archive = zipfile.ZipFile(BytesIO(file_bytes))
    except zipfile.BadZipFile as exc:
        raise ValueError("The uploaded file is not a valid .xlsx workbook") from exc

    archive_names = set(archive.namelist())
    if "xl/workbook.xml" not in archive_names:
        if "Index/Document.iwa" in archive_names:
            raise ValueError(
                "This file appears to be an Apple Numbers document renamed as .xlsx. "
                "Please export it from Numbers as a real Excel (.xlsx) file and upload that export."
            )
        raise ValueError("The uploaded file is missing Excel workbook data")

    shared_strings = _load_shared_strings(archive)
    sheet_meta = _load_sheet_metadata(archive)

    best_match: Optional[ParsedWorkbook] = None

    for sheet_name, sheet_path in sheet_meta:
        rows = _read_sheet_rows(archive, sheet_path, shared_strings)
        normalized_rows = _rows_to_records(rows)
        if normalized_rows:
            parsed = ParsedWorkbook(rows=normalized_rows, sheet_name=sheet_name)
            if best_match is None or len(parsed.rows) > len(best_match.rows):
                best_match = parsed

    if best_match is not None:
        return best_match

    raise ValueError("No holdings table was found in the workbook")


def _load_shared_strings(archive: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []

    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    strings: List[str] = []

    for item in root.findall("main:si", NS):
        fragments = [
            text_node.text or ""
            for text_node in item.findall(".//main:t", NS)
        ]
        strings.append("".join(fragments))

    return strings


def _load_sheet_metadata(archive: zipfile.ZipFile) -> List[tuple[str, str]]:
    workbook = ET.fromstring(archive.read("xl/workbook.xml"))
    rels = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))

    relationship_map = {
        rel.attrib["Id"]: _resolve_relationship_target(rel.attrib["Target"])
        for rel in rels.findall("rel:Relationship", NS)
    }

    sheets: List[tuple[str, str]] = []
    for sheet in workbook.findall("main:sheets/main:sheet", NS):
        rel_id = sheet.attrib.get(
            "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
        )
        target = relationship_map.get(rel_id)
        if target:
            sheets.append((sheet.attrib.get("name", "Sheet1"), target))

    return sheets


def _resolve_relationship_target(target: str) -> str:
    cleaned = target.lstrip("/")
    if cleaned.startswith("xl/"):
        return cleaned
    return f"xl/{cleaned}"


def _read_sheet_rows(
    archive: zipfile.ZipFile, sheet_path: str, shared_strings: List[str]
) -> List[List[str]]:
    root = ET.fromstring(archive.read(sheet_path))
    rows: List[List[str]] = []

    for row in root.findall("main:sheetData/main:row", NS):
        cells: Dict[int, str] = {}

        for cell in row.findall("main:c", NS):
            cell_ref = cell.attrib.get("r", "")
            column_index = _column_letters_to_index(re.sub(r"\d+", "", cell_ref))
            cells[column_index] = _cell_value(cell, shared_strings)

        if not cells:
            continue

        max_index = max(cells)
        rows.append([cells.get(index, "").strip() for index in range(max_index + 1)])

    return rows


def _cell_value(cell: ET.Element, shared_strings: List[str]) -> str:
    cell_type = cell.attrib.get("t")
    value = cell.find("main:v", NS)

    if cell_type == "inlineStr":
        return "".join(
            text_node.text or ""
            for text_node in cell.findall(".//main:t", NS)
        )

    if value is None or value.text is None:
        return ""

    raw = value.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (ValueError, IndexError):
            return raw

    return raw


def _column_letters_to_index(letters: str) -> int:
    index = 0
    for char in letters:
        if not char.isalpha():
            continue
        index = index * 26 + (ord(char.upper()) - 64)
    return max(index - 1, 0)


def _rows_to_records(rows: Iterable[List[str]]) -> List[Dict[str, object]]:
    header_map: Optional[Dict[int, str]] = None
    records: List[Dict[str, object]] = []

    for row in rows:
        if header_map is None:
            candidate = {
                index: HEADER_ALIASES[_normalize_header(cell)]
                for index, cell in enumerate(row)
                if _normalize_header(cell) in HEADER_ALIASES
            }
            if ESSENTIAL_HEADERS.issubset(set(candidate.values())):
                header_map = candidate
            continue

        record: Dict[str, object] = {}
        for index, key in header_map.items():
            if index < len(row):
                record[key] = row[index]

        normalized = _normalize_record(record)
        if normalized:
            records.append(normalized)

    return records


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).strip().lower())


def _normalize_record(record: Dict[str, object]) -> Optional[Dict[str, object]]:
    symbol = str(record.get("symbol", "")).strip().upper()
    if not symbol:
        return None

    isin = _clean_text(record.get("isin"))

    quantity = _to_float(record.get("quantity"))
    t1_quantity = _to_float(record.get("t1_quantity"))
    total_quantity = quantity if quantity > 0 else t1_quantity
    if total_quantity <= 0:
        return None

    avg_buy_cost = _to_float(record.get("avg_buy_cost"))
    invested_amount = _to_float(record.get("invested_amount"))
    if invested_amount <= 0:
        invested_amount = total_quantity * avg_buy_cost

    current_price = _to_float_or_none(record.get("current_price"))
    prev_close = _to_float_or_none(record.get("prev_close"))
    current_value = _to_float_or_none(record.get("current_value"))
    unrealized_pnl = _to_float_or_none(record.get("unrealized_pnl"))
    if current_value is None:
        if current_price is not None:
            current_value = total_quantity * current_price
        elif unrealized_pnl is not None:
            current_value = invested_amount + unrealized_pnl

    if unrealized_pnl is None and current_value is not None:
        unrealized_pnl = current_value - invested_amount

    if current_price is None and current_value is not None and total_quantity > 0:
        current_price = current_value / total_quantity

    one_day_change = _to_float_or_none(record.get("one_day_change"))
    if one_day_change is None and current_price is not None and prev_close is not None:
        one_day_change = (current_price - prev_close) * total_quantity

    asset_type = _normalize_asset_type(str(record.get("asset_type", "")).strip(), isin, symbol)

    return {
        "symbol": symbol,
        "company_name": _clean_text(record.get("company_name")),
        "isin": isin,
        "sector": _clean_text(record.get("sector")),
        "asset_type": asset_type,
        "quantity": total_quantity,
        "avg_buy_cost": avg_buy_cost,
        "invested_amount": invested_amount,
        "prev_close": prev_close,
        "current_price": current_price,
        "current_value": current_value,
        "one_day_change": one_day_change,
        "unrealized_pnl": unrealized_pnl,
        "currency": _clean_text(record.get("currency")) or "INR",
    }


def _normalize_asset_type(value: str, isin: Optional[str] = None, symbol: Optional[str] = None) -> str:
    normalized = _normalize_header(value)
    isin_text = (isin or "").strip().upper()
    symbol_text = (symbol or "").strip().upper()

    if isin_text.startswith("INF"):
        return "MUTUAL_FUND"

    if "fund" in normalized or "mutual" in normalized or normalized == "mf":
        return "MUTUAL_FUND"

    if "indexfund" in normalized:
        return "MUTUAL_FUND"

    if "mutual" in normalized or normalized == "mf":
        return "MUTUAL_FUND"

    if normalized == "etf" or normalized.endswith("etf"):
        return "ETF"

    if symbol_text.endswith("ETF"):
        return "ETF"

    if normalized in {"stock", "equity", "shares"}:
        return "STOCK"

    return "STOCK"


def _clean_text(value: object) -> Optional[str]:
    cleaned = str(value).strip() if value is not None else ""
    return cleaned or None


def _to_float(value: object) -> float:
    number = _to_float_or_none(value)
    return number if number is not None else 0.0


def _to_float_or_none(value: object) -> Optional[float]:
    if value is None:
        return None

    text = str(value).strip().replace(",", "")
    if not text:
        return None

    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"

    text = text.replace("%", "")
    try:
        number = float(text)
    except ValueError:
        return None

    if not math.isfinite(number):
        return None

    return number
