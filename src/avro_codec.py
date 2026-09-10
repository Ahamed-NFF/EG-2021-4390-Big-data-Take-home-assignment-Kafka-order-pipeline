"""Avro serialisation for Order records.

Wire format is Avro **single-object encoding** (Avro spec, "Single object
encoding"), which is self-describing enough to detect a schema mismatch without
running a Confluent Schema Registry:

    +--------+--------+------------------------+---------------------------+
    |  0xC3  |  0x01  | CRC-64-AVRO fingerprint|  Avro binary datum        |
    |        |        | (8 bytes, little-endian)|  (no per-record schema)  |
    +--------+--------+------------------------+---------------------------+
      <---- 2-byte marker ---->  <-- 8 bytes -->   <-- remaining bytes -->

Only the 10-byte header rides along with each record, not the schema itself,
which is the whole point of Avro on a high-volume topic: the schema is agreed
out of band (``schemas/order.avsc``) and the payload stays compact.

The fingerprint lets the consumer pick the *writer's* schema and hand both
writer and reader schema to the decoder, so Avro's schema-resolution rules
apply and an evolved schema still reads old bytes.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

from fastavro import schemaless_reader, schemaless_writer
from fastavro.schema import fingerprint, parse_schema, to_parsing_canonical_form
from fastavro.validation import validate

from .errors import DeserializationError

#: Avro single-object encoding marker (spec-defined constant).
MAGIC_BYTES = b"\xc3\x01"
FINGERPRINT_SIZE = 8
HEADER_SIZE = len(MAGIC_BYTES) + FINGERPRINT_SIZE
FINGERPRINT_ALGORITHM = "CRC-64-AVRO"


def schema_fingerprint(parsed_schema: dict) -> bytes:
    """CRC-64-AVRO fingerprint of a parsed schema, as 8 little-endian bytes."""
    canonical = to_parsing_canonical_form(parsed_schema)
    digest_hex = fingerprint(canonical, FINGERPRINT_ALGORITHM)
    return int(digest_hex, 16).to_bytes(FINGERPRINT_SIZE, byteorder="little")


class SchemaRegistry:
    """Fingerprint-keyed schema store.

    A deliberately small stand-in for Confluent Schema Registry: it resolves a
    writer's fingerprint to the schema that produced the bytes. Registering
    several ``.avsc`` files lets one consumer decode records written by more
    than one version of the schema.
    """

    def __init__(self) -> None:
        self._by_fingerprint: dict[bytes, dict] = {}

    def register(self, parsed_schema: dict) -> bytes:
        fp = schema_fingerprint(parsed_schema)
        self._by_fingerprint[fp] = parsed_schema
        return fp

    def register_file(self, schema_path: str | Path) -> bytes:
        parsed = parse_schema(json.loads(Path(schema_path).read_text(encoding="utf-8")))
        return self.register(parsed)

    def lookup(self, fp: bytes) -> dict:
        try:
            return self._by_fingerprint[fp]
        except KeyError:
            raise DeserializationError(
                f"unknown schema fingerprint {fp.hex()}; "
                f"registered: {[k.hex() for k in self._by_fingerprint]}"
            ) from None

    def __contains__(self, fp: bytes) -> bool:
        return fp in self._by_fingerprint


class AvroCodec:
    """Encodes and decodes Order records against one reader schema."""

    def __init__(self, schema_path: str | Path, registry: SchemaRegistry | None = None) -> None:
        self.schema_path = Path(schema_path)
        raw = json.loads(self.schema_path.read_text(encoding="utf-8"))
        self.schema = parse_schema(raw)
        self.registry = registry or SchemaRegistry()
        self.fingerprint = self.registry.register(self.schema)

    @property
    def fingerprint_hex(self) -> str:
        return self.fingerprint.hex()

    # --- encode -------------------------------------------------------------
    def encode(self, record: dict) -> bytes:
        """Serialise a record to single-object-encoded Avro bytes.

        Validation runs first so a producer bug surfaces here, with the field
        name in the message, rather than as unreadable bytes on the topic.
        """
        if not validate(record, self.schema, raise_errors=False):
            # Re-run with errors on to get the specific field in the message.
            validate(record, self.schema, raise_errors=True)

        buffer = io.BytesIO()
        schemaless_writer(buffer, self.schema, record)
        return MAGIC_BYTES + self.fingerprint + buffer.getvalue()

    # --- decode -------------------------------------------------------------
    def decode(self, payload: bytes | None) -> dict:
        """Deserialise single-object-encoded Avro bytes back to a dict.

        Every failure mode here is permanent — bad bytes stay bad — so they all
        raise :class:`DeserializationError` and the caller routes straight to
        the DLQ without retrying.
        """
        if payload is None:
            raise DeserializationError("record value is null (tombstone)")

        if len(payload) < HEADER_SIZE:
            raise DeserializationError(
                f"payload is {len(payload)} bytes, shorter than the "
                f"{HEADER_SIZE}-byte single-object header"
            )

        if payload[:2] != MAGIC_BYTES:
            raise DeserializationError(
                f"bad magic bytes {payload[:2].hex()}, expected {MAGIC_BYTES.hex()} "
                "(payload is not Avro single-object encoded)"
            )

        writer_fp = payload[2:HEADER_SIZE]
        writer_schema = self.registry.lookup(writer_fp)

        try:
            buffer = io.BytesIO(payload[HEADER_SIZE:])
            return schemaless_reader(buffer, writer_schema, self.schema)
        except DeserializationError:
            raise
        except Exception as exc:
            raise DeserializationError(
                f"Avro decode failed: {type(exc).__name__}: {exc}"
            ) from exc


def default_codec() -> AvroCodec:
    """Codec for ``schemas/order.avsc`` — the schema the pipeline runs on."""
    from .config import ORDER_SCHEMA_PATH

    return AvroCodec(ORDER_SCHEMA_PATH)
