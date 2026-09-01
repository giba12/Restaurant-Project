import os
import random
from simulator_base import run_simulator

RESTAURANT_ID = os.environ.get("RESTAURANT_ID", "sim-restaurant-01")
MQTT_BROKER = os.environ.get("MQTT_BROKER", "mosquitto.kafka.svc.cluster.local")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MEAN_INTERVAL_SECONDS = float(os.environ.get("MEAN_INTERVAL_SECONDS", "8"))

STATIONS = ["bus-station-01", "bus-station-02", "expo-station-01"]
MENU_ITEMS = ["item-burger-01", "item-fries-01", "item-salad-02", "item-pasta-04", "item-steak-07"]
PORTION_VARIANTS = ["standard", "half", "large", "unknown"]


def generate_plate_waste_event() -> dict:
    n_items = random.randint(0, 3)
    return {
        "event_type": "PlateWasteEvent",
        "restaurant_id": RESTAURANT_ID,
        "station_id": random.choice(STATIONS),
        "table_id": f"table-{random.randint(1, 24)}" if random.random() > 0.15 else None,
        "estimated_waste_grams": round(random.uniform(0, 350), 1),
        "plate_item_ids": random.sample(MENU_ITEMS, n_items) if n_items else [],
        "confounder_flags": {
            "to_go_container_used": random.random() < 0.2,
            "declared_dietary_restriction": random.random() < 0.1,
            "portion_size_variant": random.choice(PORTION_VARIANTS),
        },
        "image_ref": None,
    }


if __name__ == "__main__":
    run_simulator(
        sensor_name="plate-waste",
        mqtt_topic="sensors/plate-waste",
        schema_path="/app/schemas/PlateWasteEvent.schema.json",
        mqtt_broker=MQTT_BROKER,
        mqtt_port=MQTT_PORT,
        generate_event_fn=generate_plate_waste_event,
        mean_interval_seconds=MEAN_INTERVAL_SECONDS,
    )
