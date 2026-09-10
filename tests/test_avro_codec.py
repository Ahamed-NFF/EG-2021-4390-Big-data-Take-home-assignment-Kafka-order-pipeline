"""Avro serialisation: round-trip, wire format, failure modes, evolution."""

import json
import struct

import pytest
from fastavro.schema import parse_schema

from src.avro_codec import (
    HEADER_SIZE,
    MAGIC_BYTES,
    AvroCodec,
    SchemaRegistry,
    schema_fingerprint,
)
from src.config import ORDER_SCHEMA_PATH, SCHEMA_DIR
from src.errors import DeserializationError

V2_SCHEMA_PATH = SCHEMA_DIR / "order_v2.avsc"


@pytest.fixture
def codec():
    return AvroCodec(ORDER_SCHEMA_PATH)


@pytest.fixture
def order():
    return {"orderId": "1001", "product": "Item1", "price": 99.95}


def test_round_trip_preserves_fields(codec, order):
    decoded = codec.decode(codec.encode(order))
    assert decoded["orderId"] == order["orderId"]
    assert decoded["product"] == order["product"]
    # Avro `float` is 32-bit, so a float64 literal is not preserved exactly.
    assert decoded["price"] == pytest.approx(order["price"], rel=1e-6)


def test_wire_format_is_avro_single_object_encoding(codec, order):
    payload = codec.encode(order)
    assert payload[:2] == MAGIC_BYTES == b"\xc3\x01"

    # Fingerprint is 8 bytes, little-endian, per the Avro spec.
    fingerprint = payload[2:HEADER_SIZE]
    assert len(fingerprint) == 8
    assert fingerprint == codec.fingerprint
    assert struct.unpack("<Q", fingerprint)[0] == int.from_bytes(fingerprint, "little")

    # Header is exactly 10 bytes; everything after it is the datum itself.
    assert HEADER_SIZE == 10
    assert len(payload) > HEADER_SIZE


def test_payload_is_compact_because_schema_is_not_inlined(codec, order):
    """The point of Avro: the schema does not ride along with each record."""
    payload = codec.encode(order)
    json_size = len(json.dumps(order).encode())
    schema_size = len(ORDER_SCHEMA_PATH.read_text(encoding="utf-8"))
    assert len(payload) < json_size
    assert len(payload) < schema_size / 4


def test_fingerprint_is_stable_across_instances():
    assert AvroCodec(ORDER_SCHEMA_PATH).fingerprint == AvroCodec(ORDER_SCHEMA_PATH).fingerprint


def test_fingerprint_differs_between_schema_versions():
    assert AvroCodec(ORDER_SCHEMA_PATH).fingerprint != AvroCodec(V2_SCHEMA_PATH).fingerprint


# --- failure modes: every one of these must be permanent, never retried ------

def test_decode_rejects_null_payload(codec):
    with pytest.raises(DeserializationError, match="null"):
        codec.decode(None)


def test_decode_rejects_short_payload(codec):
    with pytest.raises(DeserializationError, match="shorter than"):
        codec.decode(b"\xc3\x01\x00")


def test_decode_rejects_bad_magic_bytes(codec):
    corrupt = b"\xde\xad" + b"\x00" * 20
    with pytest.raises(DeserializationError, match="magic"):
        codec.decode(corrupt)


def test_decode_rejects_unknown_schema_fingerprint(codec):
    unknown = MAGIC_BYTES + b"\x01\x02\x03\x04\x05\x06\x07\x08" + b"\x00" * 8
    with pytest.raises(DeserializationError, match="unknown schema fingerprint"):
        codec.decode(unknown)


def test_decode_rejects_truncated_body(codec, order):
    payload = codec.encode(order)
    with pytest.raises(DeserializationError):
        codec.decode(payload[: HEADER_SIZE + 2])


def test_encode_rejects_record_missing_a_field(codec):
    with pytest.raises(Exception):
        codec.encode({"orderId": "1001", "product": "Item1"})


def test_encode_rejects_wrong_type(codec):
    with pytest.raises(Exception):
        codec.encode({"orderId": "1001", "product": "Item1", "price": "free"})


def test_encode_accepts_negative_price_because_avro_cannot_express_business_rules(codec):
    """Schema-valid but business-invalid: this is why validation is separate."""
    payload = codec.encode({"orderId": "1001", "product": "Item1", "price": -5.0})
    assert codec.decode(payload)["price"] == pytest.approx(-5.0)


# --- schema evolution -------------------------------------------------------

def test_v2_reader_decodes_v1_bytes_using_defaults():
    """Backward compatibility: new consumer, old data still on the topic."""
    registry = SchemaRegistry()
    v1 = AvroCodec(ORDER_SCHEMA_PATH, registry)
    v2 = AvroCodec(V2_SCHEMA_PATH, registry)

    v1_bytes = v1.encode({"orderId": "1001", "product": "Item1", "price": 10.0})
    decoded = v2.decode(v1_bytes)

    assert decoded["orderId"] == "1001"
    assert decoded["quantity"] == 1       # from the schema default
    assert decoded["currency"] == "USD"   # from the schema default


def test_v1_reader_ignores_fields_added_in_v2():
    """Forward compatibility: old consumer, new producer already deployed."""
    registry = SchemaRegistry()
    v1 = AvroCodec(ORDER_SCHEMA_PATH, registry)
    v2 = AvroCodec(V2_SCHEMA_PATH, registry)

    v2_bytes = v2.encode({"orderId": "1002", "product": "Item2", "price": 20.0,
                          "quantity": 7, "currency": "EUR"})
    decoded = v1.decode(v2_bytes)

    assert decoded == {"orderId": "1002", "product": "Item2",
                       "price": pytest.approx(20.0)}
    assert "quantity" not in decoded


def test_codec_cannot_decode_a_schema_it_never_registered():
    """Isolated registries are exactly how an unknown-fingerprint DLQ happens."""
    v2_only = AvroCodec(V2_SCHEMA_PATH, SchemaRegistry())
    v1_only = AvroCodec(ORDER_SCHEMA_PATH, SchemaRegistry())

    v2_bytes = v2_only.encode({"orderId": "1", "product": "p", "price": 1.0,
                               "quantity": 1, "currency": "USD"})
    with pytest.raises(DeserializationError, match="unknown schema fingerprint"):
        v1_only.decode(v2_bytes)


# --- registry ---------------------------------------------------------------

def test_registry_lookup_returns_registered_schema():
    registry = SchemaRegistry()
    fp = registry.register_file(ORDER_SCHEMA_PATH)
    assert fp in registry
    assert registry.lookup(fp)["name"] == "com.bigdata.assignment.orders.Order"


def test_fingerprint_ignores_doc_strings_and_field_order():
    """Canonical form strips docs, so a comment change is not a new schema."""
    base = json.loads(ORDER_SCHEMA_PATH.read_text(encoding="utf-8"))
    stripped = {
        "type": base["type"],
        "name": base["name"],
        "namespace": base["namespace"],
        "fields": [{"name": f["name"], "type": f["type"]} for f in base["fields"]],
    }
    assert schema_fingerprint(parse_schema(base)) == schema_fingerprint(parse_schema(stripped))
