"""Device configuration/state documents in MongoDB (`connectedfleet.device_state`, `_id = device_id`)."""

from __future__ import annotations

DATABASE = "connectedfleet"
COLLECTION = "device_state"


def collection(uri: str):
    from pymongo import MongoClient

    client = MongoClient(uri, serverSelectionTimeoutMS=10_000)
    return client[DATABASE][COLLECTION]


def upsert_state(coll, device_id: str, profile: str, interval_s: int, mode: str, firmware: str) -> None:
    coll.replace_one(
        {"_id": device_id},
        {"_id": device_id, "config_profile": profile, "sampling_interval_s": interval_s,
         "reporting_mode": mode, "desired_firmware": firmware},
        upsert=True,
    )


def patch_state(coll, device_id: str, changes: dict) -> int:
    """Partial update ($set): the change stream carries only the patch; the full document is looked up later."""
    return coll.update_one({"_id": device_id}, {"$set": changes}).modified_count


def delete_state(coll, device_id: str) -> int:
    return coll.delete_one({"_id": device_id}).deleted_count


SEED = (
    ("sensor-00001", "cold-chain", 30, "normal", "3.2.1"),
    ("sensor-00002", "cold-chain", 30, "normal", "3.2.1"),
    ("sensor-00003", "standard", 60, "power_saving", "3.2.0"),
    ("sensor-00004", "standard", 60, "normal", "3.2.0"),
    ("sensor-00005", "diagnostic", 5, "diagnostic", "3.3.0-rc1"),
)


def seed(coll) -> int:
    for row in SEED:
        upsert_state(coll, *row)
    return len(SEED)
