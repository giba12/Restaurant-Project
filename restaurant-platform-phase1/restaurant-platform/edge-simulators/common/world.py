"""
Shared simulation world model.

All four simulators import this module so that station IDs, staff IDs,
table IDs, and menu item IDs are drawn from the same fixed domains.
This is what makes PlateWasteEvent, POSTransactionEvent,
ServiceTimingEvent, and StaffShiftEvent joinable later by
restaurant_id / table_id / station_id / staff_id in the causal engine
(Phase 5). Divergent per-simulator ID spaces would silently break that
joinability without raising any error now, so this module is the single
source of truth rather than each simulator inventing its own IDs.
"""

RESTAURANT_ID = "rest-001"

STATIONS = [
    "station-grill",
    "station-saute",
    "station-salad",
    "station-expo",
    "station-bar",
    "station-bussing-01",
]

TABLES = [f"table-{i:02d}" for i in range(1, 21)]  # table-01 .. table-20

STAFF_ROSTER = [
    {"staff_id": "staff-001", "role": "server"},
    {"staff_id": "staff-002", "role": "server"},
    {"staff_id": "staff-003", "role": "server"},
    {"staff_id": "staff-004", "role": "line_cook"},
    {"staff_id": "staff-005", "role": "line_cook"},
    {"staff_id": "staff-006", "role": "expo"},
    {"staff_id": "staff-007", "role": "host"},
    {"staff_id": "staff-008", "role": "bartender"},
    {"staff_id": "staff-009", "role": "dishwasher"},
    {"staff_id": "staff-010", "role": "manager"},
]

# menu_item_id -> (display name [unused by schema, kept for readability], unit_price_cents)
MENU = {
    "menu-burger-classic": 1495,
    "menu-burger-deluxe": 1795,
    "menu-salad-caesar": 1195,
    "menu-salad-cobb": 1395,
    "menu-pasta-alfredo": 1695,
    "menu-steak-8oz": 2895,
    "menu-fish-tacos": 1595,
    "menu-soup-of-day": 895,
    "menu-fries-side": 595,
    "menu-dessert-cake": 795,
    "menu-drink-soda": 395,
    "menu-drink-beer": 695,
}

MENU_ITEM_IDS = list(MENU.keys())

SCHEMA_VERSION = "1.0.0"
