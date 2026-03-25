import base64

import pytest

from app.importers import decode_base64_document, parse_xlsx_holdings


def test_decode_base64_document_supports_raw_and_data_uri_payloads():
    payload = b"hello workbook"
    raw = base64.b64encode(payload).decode("utf-8")
    data_uri = f"data:application/octet-stream;base64,{raw}"

    assert decode_base64_document(raw) == payload
    assert decode_base64_document(data_uri) == payload


def test_parse_xlsx_holdings_rejects_non_xlsx_payload():
    with pytest.raises(ValueError, match="valid .xlsx workbook"):
        parse_xlsx_holdings(b"not-an-xlsx")
