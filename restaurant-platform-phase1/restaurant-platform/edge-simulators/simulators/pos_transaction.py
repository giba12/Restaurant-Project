import random

from common import world
from common.ids import new_event_id, now_iso
from common.runtime import Simulator

PAYMENT_METHODS = ["card", "card", "card", "cash", "mobile_wallet", "gift_card"]


def generate_event() -> dict:
    servers = [s for s in world.STAFF_ROSTER if s["role"] == "server"]
    server = random.choice(servers)

    n_items = random.randint(1, 5)
    chosen_items = random.choices(world.MENU_ITEM_IDS, k=n_items)

    line_items = []
    total_cents = 0
    for menu_item_id in chosen_items:
        qty = random.choices([1, 2, 3], weights=[0.7, 0.25, 0.05])[0]
        unit_price = world.MENU[menu_item_id]
        voided = random.random() < 0.03
        line_items.append({
            "menu_item_id": menu_item_id,
            "quantity": qty,
            "unit_price_cents": unit_price,
            "modifiers": [],
            "voided": voided,
        })
        if not voided:
            total_cents += qty * unit_price

    discount_cents = 0
    if random.random() < 0.08:
        discount_cents = round(total_cents * random.choice([0.10, 0.15, 0.20]))
        total_cents -= discount_cents

    return {
        "event_id": new_event_id(),
        "event_type": "POSTransactionEvent",
        "schema_version": world.SCHEMA_VERSION,
        "source_id": "sim-pos-01",
        "source_kind": "simulated",
        "timestamp": now_iso(),
        "restaurant_id": world.RESTAURANT_ID,
        "transaction_id": new_event_id(),
        "table_id": random.choice(world.TABLES),
        "server_staff_id": server["staff_id"],
        "line_items": line_items,
        "total_amount_cents": max(total_cents, 0),
        "currency": "USD",
        "payment_method": random.choice(PAYMENT_METHODS),
        "discount_applied_cents": discount_cents,
    }


def main():
    sim = Simulator(
        sensor_type="pos-transaction",
        schema_filename="POSTransactionEvent.schema.json",
        mqtt_topic="sensors/pos-transaction",
    )
    sim.run_forever(generate_event)


if __name__ == "__main__":
    main()
