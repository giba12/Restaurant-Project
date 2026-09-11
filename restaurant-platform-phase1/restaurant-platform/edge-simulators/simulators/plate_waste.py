import random

from common import world
from common.ids import new_event_id, now_iso
from common.runtime import Simulator

PORTION_VARIANTS = ["standard", "standard", "standard", "half", "large", "unknown"]


def generate_event() -> dict:
    n_items = random.randint(1, 3)
    plate_item_ids = random.sample(world.MENU_ITEM_IDS, k=n_items)

    to_go = random.random() < 0.15
    dietary = random.random() < 0.10

    # A to-go box strongly suppresses observed waste; this is exactly the
    # kind of confound the causal engine (Phase 5) is meant to control for,
    # so the simulator must actually encode the relationship rather than
    # generating waste_grams independently of confounder_flags.
    base_waste = random.gauss(mu=120, sigma=45)
    if to_go:
        base_waste *= 0.25
    waste_grams = max(0.0, round(base_waste, 1))

    return {
        "event_id": new_event_id(),
        "event_type": "PlateWasteEvent",
        "schema_version": world.SCHEMA_VERSION,
        "source_id": "sim-plate-cam-01",
        "source_kind": "simulated",
        "timestamp": now_iso(),
        "restaurant_id": world.RESTAURANT_ID,
        "station_id": "station-bussing-01",
        "table_id": random.choice(world.TABLES) if random.random() < 0.7 else None,
        "estimated_waste_grams": waste_grams,
        "plate_item_ids": plate_item_ids,
        "confounder_flags": {
            "to_go_container_used": to_go,
            "declared_dietary_restriction": dietary,
            "portion_size_variant": random.choice(PORTION_VARIANTS),
        },
        "image_ref": None,
    }


def main():
    sim = Simulator(
        sensor_type="plate-waste",
        schema_filename="PlateWasteEvent.schema.json",
        mqtt_topic="sensors/plate-waste",
    )
    sim.run_forever(generate_event)


if __name__ == "__main__":
    main()