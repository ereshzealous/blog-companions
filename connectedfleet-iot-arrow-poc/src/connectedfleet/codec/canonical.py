"""Decode + validate canonical Kafka values for any canonical topic (shared by lanes B and C).

`cf.telemetry.v1` goes through the frozen Lab 01 codec, so the lanes and Lab 01
measure the same code. The other topics use the generic contract validator.
Both produce typed row tuples in contract field order.
"""

from __future__ import annotations

from connectedfleet import topics
from connectedfleet.codec import telemetry as tel_codec
from connectedfleet.contracts import canonical


def decode(values: list[bytes], first_index: int = 0):
    """JSON-decode values; identical for every topic (delegates to the Lab 01 decoder)."""
    return tel_codec.decode(values, first_index)


def validate(topic: str, decoded: list[tuple[int, dict]]) -> tuple[list[tuple], list[tel_codec.Reject]]:
    if topic == topics.TELEMETRY or topic.startswith("cfbench."):
        rows, rejects, _stats = tel_codec.validate(decoded)
        return rows, rejects
    contract = canonical.CONTRACTS[topic]
    rows: list[tuple] = []
    rejects: list[tel_codec.Reject] = []
    for index, obj in decoded:
        try:
            rows.append(canonical.validate(contract, obj))
        except canonical.ContractError as err:
            rejects.append(tel_codec.Reject(index, err.code, err.field))
    return rows, rejects


def contract_for(topic: str) -> canonical.TopicContract:
    """Benchmark topics carry canonical telemetry."""
    if topic.startswith("cfbench."):
        return canonical.TELEMETRY
    return canonical.CONTRACTS[topic]
