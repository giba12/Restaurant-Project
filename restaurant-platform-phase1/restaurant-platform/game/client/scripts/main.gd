extends Control
## Restaurant Shift -- single-screen prototype.
##
## Pick a role (line cook or server), clock in, and work the tickets that are
## waiting on YOU. The rest of the kitchen -- the stages your role doesn't own --
## is done by a crew inside the bridge, after a delay, so your speed moves the
## whole chain: a slow cook delays the server, a slow server delays the guest.
## Every action goes to game-bridge, which validates it and publishes the same
## events the simulators emit (tagged source_kind "player"; the crew's are
## tagged "simulated"). The bottom panel shows the latest LLM-narrated finding
## from the platform's dashboard API, closing the loop: your shift -> the
## pipeline -> a finding.
##
## The bridge is the authority on rules (who may do which stage, at which
## station), on when tickets arrive, and on what the crew does; this script only
## polls the ticket board, presents it, and reports what the bridge rejected.
##
## When a shift ends, a report compares it two ways, both read from the
## platform's dashboard API (GET /api/comparison): your response times against
## the crew's over the same shift (same clock), and game tickets against the
## simulated restaurant's.
##
## A fourth option in the role picker, "guest", is not staff at all: pick a
## table and build an order from the menu, place it (this is what fires the
## kitchen ticket), watch it move through the same stages the staff work, and
## pay once it is delivered. No clock-in, no role gate -- the bridge does not
## require either for a guest.

const BridgeClient := preload("res://scripts/bridge_client.gd")

signal world_loaded

# Label for the action that moves a ticket INTO the given stage.
const STAGE_ACTIONS := {
	"cook_started": "Start cooking",
	"plated": "Plate it",
	"picked_up_by_server": "Pick up",
	"delivered": "Deliver",
}
const LINE_COOK := "line_cook"
const GUEST_ROLE := "guest"  # not a bridge role -- a client-side option that skips staff-shift entirely
const TICKET_POLL_SECONDS := 1.5
const GUEST_POLL_SECONDS := 2.0
const FINDINGS_POLL_SECONDS := 15.0
const WORLD_RETRY_SECONDS := 3.0
const METRIC_LABELS := {
	"time_to_cook_start_ms": "Time to cook start",
	"cook_duration_ms": "Cook time",
	"pickup_delay_ms": "Pickup delay",
	"service_delay_ms": "Service delay",
	"total_ticket_duration_ms": "Whole ticket",
}
const SLOW_STAGE_SECONDS := 15.0

var bridge: BridgeClient
var dashboard_url := "http://127.0.0.1:8080"

var world: Dictionary = {}
var stages: Array = []
var player_id := ""
var source_id := ""        # the bridge's name for this player's events, from the clock-in event
var shift_started := ""    # clock-in timestamp (ISO 8601), where the shift report's window starts
var role := ""
var station := ""
var on_shift := false
var is_guest := false
var guest_table := ""
var guest_cart: Dictionary = {}       # menu_item_id -> quantity, while building an order
var cart_spinboxes: Dictionary = {}   # menu_item_id -> SpinBox
var guest_delivered := false
var guest_paid := false
var tickets: Dictionary = {}  # ticket_id -> {card, info, age, button, stage_started_ms, busy, table, station_id, next_stage, can_act, waiting_on}
var actions_done := 0
var response_ms_total := 0

var status_label: Label
var setup_panel: VBoxContainer
var shift_panel: VBoxContainer
var name_edit: LineEdit
var role_picker: OptionButton
var station_row: HBoxContainer
var station_picker: OptionButton
var clock_in_button: Button
var clock_out_button: Button
var shift_header: Label
var stats_label: Label
var board_hint: Label
var report_label: Label
var tickets_box: VBoxContainer
var findings_label: Label
var poll_timer: Timer
var findings_timer: Timer
var retry_timer: Timer
var guest_poll_timer: Timer

var table_row: HBoxContainer
var table_picker: OptionButton
var guest_panel: VBoxContainer
var guest_header: Label
var cart_section: VBoxContainer
var cart_items_box: VBoxContainer
var cart_total_label: Label
var place_order_button: Button
var status_section: VBoxContainer
var order_status_label: Label
var pay_row: HBoxContainer
var payment_picker: OptionButton
var pay_button: Button


func _ready() -> void:
	bridge = BridgeClient.new()
	add_child(bridge)
	var bridge_env := OS.get_environment("BRIDGE_URL")
	if bridge_env != "":
		bridge.base_url = bridge_env
	var dash_env := OS.get_environment("DASHBOARD_URL")
	if dash_env != "":
		dashboard_url = dash_env

	# Closing the window mid-shift should clock the player out, otherwise the
	# digital twin shows them on shift forever.
	get_tree().set_auto_accept_quit(false)

	_build_ui()

	poll_timer = _make_timer(TICKET_POLL_SECONDS, _refresh_tickets)
	guest_poll_timer = _make_timer(GUEST_POLL_SECONDS, _refresh_guest_status)
	findings_timer = _make_timer(FINDINGS_POLL_SECONDS, _refresh_findings)
	retry_timer = _make_timer(WORLD_RETRY_SECONDS, _load_world)
	findings_timer.start()
	_refresh_findings()
	_load_world()


func _notification(what: int) -> void:
	if what == NOTIFICATION_WM_CLOSE_REQUEST:
		if on_shift:
			await _on_clock_out_pressed(false)  # no report: the window is closing
		get_tree().quit()


func _process(_delta: float) -> void:
	var now := Time.get_ticks_msec()
	for id in tickets:
		var t: Dictionary = tickets[id]
		var stage_secs := float(now - int(t.stage_started_ms)) / 1000.0
		var age: Label = t.age
		age.text = "%ds" % int(stage_secs)
		var late: bool = t.can_act and stage_secs > SLOW_STAGE_SECONDS
		age.add_theme_color_override("font_color", Color(0.95, 0.4, 0.35) if late else Color(0.85, 0.85, 0.85))


# ---------------------------------------------------------------- UI

func _make_timer(seconds: float, callback: Callable) -> Timer:
	var t := Timer.new()
	t.wait_time = seconds
	t.one_shot = false
	t.timeout.connect(callback)
	add_child(t)
	return t


func _build_ui() -> void:
	var margin := MarginContainer.new()
	margin.set_anchors_preset(Control.PRESET_FULL_RECT)
	for side in ["margin_left", "margin_right", "margin_top", "margin_bottom"]:
		margin.add_theme_constant_override(side, 20)
	add_child(margin)

	var root_box := VBoxContainer.new()
	root_box.add_theme_constant_override("separation", 12)
	margin.add_child(root_box)

	var title := Label.new()
	title.text = "Restaurant Shift"
	title.add_theme_font_size_override("font_size", 28)
	root_box.add_child(title)

	status_label = Label.new()
	status_label.text = "Connecting to the bridge..."
	status_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	root_box.add_child(status_label)

	report_label = Label.new()
	report_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	report_label.visible = false
	root_box.add_child(report_label)

	# --- setup: name, role, station, clock in
	setup_panel = VBoxContainer.new()
	setup_panel.add_theme_constant_override("separation", 8)
	root_box.add_child(setup_panel)

	var name_row := _labeled_row("Your name")
	name_edit = LineEdit.new()
	name_edit.placeholder_text = "e.g. ana"
	name_edit.custom_minimum_size.x = 240
	name_row.add_child(name_edit)
	setup_panel.add_child(name_row)

	var role_row := _labeled_row("Role")
	role_picker = OptionButton.new()
	role_picker.custom_minimum_size.x = 240
	role_picker.item_selected.connect(_on_role_selected)
	role_row.add_child(role_picker)
	setup_panel.add_child(role_row)

	station_row = _labeled_row("Station")
	station_picker = OptionButton.new()
	station_picker.custom_minimum_size.x = 240
	station_row.add_child(station_picker)
	setup_panel.add_child(station_row)

	table_row = _labeled_row("Table")
	table_picker = OptionButton.new()
	table_picker.custom_minimum_size.x = 240
	table_row.add_child(table_picker)
	table_row.visible = false
	setup_panel.add_child(table_row)

	clock_in_button = Button.new()
	clock_in_button.text = "Clock in"
	clock_in_button.disabled = true
	clock_in_button.custom_minimum_size = Vector2(160, 40)
	clock_in_button.size_flags_horizontal = Control.SIZE_SHRINK_BEGIN
	clock_in_button.pressed.connect(_on_clock_in_pressed)
	setup_panel.add_child(clock_in_button)

	# --- shift: ticket board
	shift_panel = VBoxContainer.new()
	shift_panel.add_theme_constant_override("separation", 8)
	shift_panel.size_flags_vertical = Control.SIZE_EXPAND_FILL
	shift_panel.visible = false
	root_box.add_child(shift_panel)

	var header_row := HBoxContainer.new()
	shift_header = Label.new()
	shift_header.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	clock_out_button = Button.new()
	clock_out_button.text = "Clock out"
	clock_out_button.pressed.connect(_on_clock_out_pressed)
	header_row.add_child(shift_header)
	header_row.add_child(clock_out_button)
	shift_panel.add_child(header_row)

	stats_label = Label.new()
	shift_panel.add_child(stats_label)

	board_hint = Label.new()
	board_hint.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	shift_panel.add_child(board_hint)

	tickets_box = VBoxContainer.new()
	tickets_box.add_theme_constant_override("separation", 6)
	shift_panel.add_child(tickets_box)

	# --- guest: build an order, place it, watch it, pay
	guest_panel = VBoxContainer.new()
	guest_panel.add_theme_constant_override("separation", 8)
	guest_panel.visible = false
	root_box.add_child(guest_panel)

	var guest_header_row := HBoxContainer.new()
	guest_header = Label.new()
	guest_header.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	var guest_leave_button := Button.new()
	guest_leave_button.text = "Leave"
	guest_leave_button.pressed.connect(_on_guest_leave_pressed)
	guest_header_row.add_child(guest_header)
	guest_header_row.add_child(guest_leave_button)
	guest_panel.add_child(guest_header_row)

	cart_section = VBoxContainer.new()
	cart_section.add_theme_constant_override("separation", 4)
	guest_panel.add_child(cart_section)

	cart_items_box = VBoxContainer.new()  # one row per menu item, built once the menu loads
	cart_items_box.add_theme_constant_override("separation", 2)
	cart_section.add_child(cart_items_box)

	cart_total_label = Label.new()
	cart_section.add_child(cart_total_label)

	place_order_button = Button.new()
	place_order_button.text = "Place order"
	place_order_button.disabled = true
	place_order_button.custom_minimum_size = Vector2(160, 36)
	place_order_button.size_flags_horizontal = Control.SIZE_SHRINK_BEGIN
	place_order_button.pressed.connect(_on_place_order_pressed)
	cart_section.add_child(place_order_button)

	status_section = VBoxContainer.new()
	status_section.add_theme_constant_override("separation", 8)
	status_section.visible = false
	guest_panel.add_child(status_section)

	order_status_label = Label.new()
	order_status_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	status_section.add_child(order_status_label)

	pay_row = HBoxContainer.new()
	pay_row.add_theme_constant_override("separation", 8)
	pay_row.visible = false
	payment_picker = OptionButton.new()
	pay_button = Button.new()
	pay_button.text = "Pay"
	pay_button.pressed.connect(_on_pay_pressed)
	pay_row.add_child(payment_picker)
	pay_row.add_child(pay_button)
	status_section.add_child(pay_row)

	# --- findings from the platform
	root_box.add_child(HSeparator.new())
	var findings_title := Label.new()
	findings_title.text = "Latest finding from the platform"
	findings_title.add_theme_font_size_override("font_size", 16)
	root_box.add_child(findings_title)
	findings_label = Label.new()
	findings_label.text = "Checking..."
	findings_label.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
	root_box.add_child(findings_label)


func _labeled_row(caption: String) -> HBoxContainer:
	var row := HBoxContainer.new()
	row.add_theme_constant_override("separation", 8)
	var label := Label.new()
	label.text = caption
	label.custom_minimum_size.x = 90
	row.add_child(label)
	return row


func _set_status(text: String) -> void:
	status_label.text = text


func _update_stats() -> void:
	var avg := 0.0 if actions_done == 0 else float(response_ms_total) / actions_done / 1000.0
	stats_label.text = "Actions: %d    Average time a ticket waited for you: %.1fs" % [actions_done, avg]


func _build_cart_rows(menu: Dictionary) -> void:
	for menu_item_id in menu:
		var row := HBoxContainer.new()
		row.add_theme_constant_override("separation", 8)
		var label := Label.new()
		label.text = "%s   $%.2f" % [str(menu_item_id).trim_prefix("menu-").replace("-", " "), float(menu[menu_item_id]) / 100.0]
		label.custom_minimum_size.x = 260
		var spin := SpinBox.new()
		spin.min_value = 0
		spin.max_value = 5
		spin.value_changed.connect(_on_cart_changed.bind(menu_item_id))
		row.add_child(label)
		row.add_child(spin)
		cart_items_box.add_child(row)
		cart_spinboxes[menu_item_id] = spin


func _on_role_selected(_index: int) -> void:
	var selected := role_picker.get_item_text(role_picker.selected)
	station_row.visible = selected == LINE_COOK
	table_row.visible = selected == GUEST_ROLE
	clock_in_button.text = "Sit down" if selected == GUEST_ROLE else "Clock in"


func _short(id: String) -> String:
	return id.trim_prefix("station-")


# ---------------------------------------------------------------- bridge

func _load_world() -> void:
	var r: Dictionary = await bridge.get_world()
	if not r.ok:
		clock_in_button.disabled = true
		_set_status("Can't reach the game bridge at %s -- is the platform running (docker compose up)? Retrying..." % bridge.base_url)
		if retry_timer.is_stopped():
			retry_timer.start()
		return
	retry_timer.stop()
	world = r.data
	stages = world.get("stages", [])
	if role_picker.item_count == 0:
		for role_name in world.get("playable_roles", {}).keys():
			role_picker.add_item(role_name)
		role_picker.add_item(GUEST_ROLE)  # not a bridge role; see the class doc comment
		for s in world.get("playable_stations", []):
			station_picker.add_item(s)
		for t in world.get("tables", []):
			table_picker.add_item(t)
		for m in world.get("payment_methods", []):
			payment_picker.add_item(m)
		_build_cart_rows(world.get("menu", {}))
		_on_role_selected(0)
	clock_in_button.disabled = on_shift or role_picker.item_count == 0
	_set_status("Connected. Pick a role and clock in.")
	world_loaded.emit()


func _sanitize_player_id(raw: String) -> String:
	# Bridge pattern: ^[a-z0-9][a-z0-9-]{0,31}$
	var out := ""
	for c in raw.strip_edges().to_lower():
		if (c >= "a" and c <= "z") or (c >= "0" and c <= "9") or c == "-":
			out += c
		elif c == " " or c == "_":
			out += "-"
	out = out.lstrip("-").substr(0, 32)
	return out if out != "" else "player1"


func _on_clock_in_pressed() -> void:
	if on_shift or is_guest or world.is_empty() or role_picker.item_count == 0:
		return
	player_id = _sanitize_player_id(name_edit.text)
	role = role_picker.get_item_text(role_picker.selected)
	if role == GUEST_ROLE:
		_start_guest_setup()
		return
	station = station_picker.get_item_text(station_picker.selected) if role == LINE_COOK else ""
	clock_in_button.disabled = true
	var r: Dictionary = await bridge.post_staff_shift(player_id, role, "clock_in")
	if not r.ok:
		clock_in_button.disabled = false
		_set_status("Couldn't clock in: %s" % r.error)
		return
	source_id = str(r.data.source_id)
	shift_started = str(r.data.timestamp)
	report_label.visible = false
	if role == LINE_COOK:
		# A line cook works one station; the bridge only lets them act on tickets there.
		var r2: Dictionary = await bridge.post_staff_shift(player_id, role, "station_reassign", station)
		if not r2.ok:
			_set_status("Clocked in, but couldn't take the station: %s" % r2.error)
	on_shift = true
	actions_done = 0
	response_ms_total = 0
	_update_stats()
	shift_header.text = "player-%s  |  %s%s" % [player_id, role.replace("_", " "), "  |  " + _short(station) if station != "" else ""]
	_set_status("On shift. Tickets waiting on you have an enabled button; the crew handles the rest.")
	setup_panel.visible = false
	shift_panel.visible = true
	board_hint.text = "Waiting for the first ticket..."
	poll_timer.start()
	await _refresh_tickets()


# ---------------------------------------------------------------- guest: order, wait, pay

func _start_guest_setup() -> void:
	# No bridge call yet -- sitting down and building an order is purely local; only
	# "Place order" talks to the bridge (that is what actually fires the kitchen ticket).
	guest_table = table_picker.get_item_text(table_picker.selected)
	guest_cart.clear()
	guest_paid = false
	for menu_item_id in cart_spinboxes:
		var spin: SpinBox = cart_spinboxes[menu_item_id]
		spin.value = 0  # triggers _on_cart_changed, which clears guest_cart for each row
	_update_cart_total()
	is_guest = true
	report_label.visible = false
	setup_panel.visible = false
	guest_panel.visible = true
	guest_header.text = "%s  |  %s" % [player_id, guest_table]
	cart_section.visible = true
	status_section.visible = false
	_set_status("Pick what you'd like, then place your order.")


func _on_cart_changed(value: float, menu_item_id: String) -> void:
	var qty := int(value)
	if qty <= 0:
		guest_cart.erase(menu_item_id)
	else:
		guest_cart[menu_item_id] = qty
	_update_cart_total()


func _cart_total_cents() -> int:
	var total := 0
	for id in guest_cart:
		total += guest_cart[id] * int(world.menu[id])
	return total


func _on_place_order_pressed() -> void:
	if guest_cart.is_empty():
		return
	place_order_button.disabled = true
	var items: Array = []
	for id in guest_cart:
		items.append({"menu_item_id": id, "quantity": guest_cart[id]})
	var r: Dictionary = await bridge.post_guest_order(player_id, guest_table, items)
	if not is_guest:
		return  # left while the request was in flight
	if not r.ok:
		place_order_button.disabled = false
		_set_status("Couldn't place the order: %s" % r.error)
		return
	guest_delivered = false
	cart_section.visible = false
	status_section.visible = true
	pay_row.visible = false
	order_status_label.text = "Order placed. Waiting on the kitchen..."
	_set_status("Order placed. Sit tight -- this updates on its own.")
	guest_poll_timer.start()
	await _refresh_guest_status()


func _update_cart_total() -> void:
	var count := 0
	for id in guest_cart:
		count += guest_cart[id]
	cart_total_label.text = "Cart: %d item(s), $%.2f" % [count, _cart_total_cents() / 100.0]
	place_order_button.disabled = count == 0


func _refresh_guest_status() -> void:
	if not is_guest:
		return
	var r: Dictionary = await bridge.get_guest_status(player_id)
	if not is_guest or not status_section.visible:
		return  # left, or already paid, while the request was in flight
	if not r.ok:
		order_status_label.text = "Can't check your order: %s" % r.error
		return
	guest_delivered = bool(r.data.delivered)
	var total := "$%.2f" % (float(r.data.total_amount_cents) / 100.0)
	if guest_delivered:
		guest_poll_timer.stop()
		order_status_label.text = "Order delivered! Total: %s" % total
		pay_row.visible = true
	else:
		var next_stage: String = str(r.data.get("next_stage", "order_fired")).replace("_", " ")
		var secs := int(float(r.data.get("seconds_in_stage", 0.0)))
		order_status_label.text = "Waiting: %s (%ds so far)   Total so far: %s" % [next_stage, secs, total]


func _on_pay_pressed() -> void:
	pay_button.disabled = true
	var method := payment_picker.get_item_text(payment_picker.selected)
	var r: Dictionary = await bridge.post_guest_pay(player_id, method)
	pay_button.disabled = false
	if not is_guest:
		return
	if not r.ok:
		order_status_label.text = "Payment failed: %s" % r.error
		return
	order_status_label.text = "Paid $%.2f. Thanks for dining with us!" % (float(r.data.total_amount_cents) / 100.0)
	pay_row.visible = false
	guest_paid = true


func _on_guest_leave_pressed() -> void:
	# Only tell the bridge if an order was actually placed and not already paid for -- before
	# ordering there is nothing server-side to leave, and after paying the bridge has already
	# cleared the session itself.
	if status_section.visible and not guest_paid:
		await bridge.post_guest_leave(player_id)
	is_guest = false
	guest_poll_timer.stop()
	guest_panel.visible = false
	setup_panel.visible = true
	clock_in_button.text = "Clock in" if role_picker.get_item_text(role_picker.selected) != GUEST_ROLE else "Sit down"
	clock_in_button.disabled = world.is_empty()
	_set_status("Connected. Pick a role and clock in.")


func _on_clock_out_pressed(show_report: bool = true) -> void:
	if not on_shift:
		return
	on_shift = false
	poll_timer.stop()
	clock_out_button.disabled = true
	var r: Dictionary = await bridge.post_staff_shift(player_id, role, "clock_out")
	clock_out_button.disabled = false
	for id in tickets.keys():
		_remove_ticket(id)
	shift_panel.visible = false
	setup_panel.visible = true
	clock_in_button.disabled = world.is_empty()
	if r.ok:
		_set_status("Shift over. You did %d action(s). The crew finishes any open tickets. Clock in again any time." % actions_done)
		if show_report:
			await _show_report()
	else:
		_set_status("Clocked out locally, but the bridge said: %s" % r.error)


# ---------------------------------------------------------------- ticket board

func _refresh_tickets() -> void:
	if not on_shift:
		return
	var r: Dictionary = await bridge.get_tickets(player_id)
	if not on_shift:
		return  # clocked out while the request was in flight
	if not r.ok:
		_set_status("Can't read the ticket board: %s" % r.error)
		return
	var seen := {}
	for row in r.data:
		# A cook only sees their own station; a server sees the whole floor.
		if role == LINE_COOK and row.station_id != station:
			continue
		seen[row.ticket_id] = true
		if not tickets.has(row.ticket_id):
			_add_ticket(row.ticket_id)
		_apply_row(row)
	for id in tickets.keys():
		if not seen.has(id):
			_remove_ticket(id)
	board_hint.text = "" if not tickets.is_empty() else "No open tickets. They arrive every few seconds while you're on shift."


func _add_ticket(ticket_id: String) -> void:
	var card := PanelContainer.new()
	var row := HBoxContainer.new()
	row.add_theme_constant_override("separation", 12)
	card.add_child(row)

	var info := Label.new()
	info.custom_minimum_size.x = 380
	info.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	var age := Label.new()
	age.custom_minimum_size.x = 50
	var button := Button.new()
	button.custom_minimum_size = Vector2(170, 34)
	row.add_child(info)
	row.add_child(age)
	row.add_child(button)
	tickets_box.add_child(card)

	tickets[ticket_id] = {
		"card": card, "info": info, "age": age, "button": button, "busy": false,
		"stage_started_ms": Time.get_ticks_msec(), "table": "", "station_id": "",
		"next_stage": "", "can_act": false, "waiting_on": "",
	}
	button.pressed.connect(_advance.bind(ticket_id))


func _apply_row(row: Dictionary) -> void:
	var t: Dictionary = tickets[row.ticket_id]
	t.table = row.table_id
	t.station_id = row.station_id
	t.next_stage = row.next_stage
	t.can_act = row.can_act
	t.waiting_on = row.waiting_on
	t.stage_started_ms = Time.get_ticks_msec() - int(float(row.seconds_in_stage) * 1000.0)
	_refresh_ticket(row.ticket_id)


func _refresh_ticket(ticket_id: String) -> void:
	var t: Dictionary = tickets[ticket_id]
	var info: Label = t.info
	var button: Button = t.button
	info.text = "%s  ·  %s  ·  next: %s" % [t.table, _short(t.station_id), str(t.next_stage).replace("_", " ")]
	if t.can_act:
		button.text = STAGE_ACTIONS.get(t.next_stage, t.next_stage)
		button.disabled = t.busy
	else:
		button.text = "waiting on %s" % t.waiting_on
		button.disabled = true


func _remove_ticket(ticket_id: String) -> void:
	if not tickets.has(ticket_id):
		return
	var card: Control = tickets[ticket_id].card
	card.queue_free()
	tickets.erase(ticket_id)


func _advance(ticket_id: String) -> void:
	if not tickets.has(ticket_id):
		return
	var t: Dictionary = tickets[ticket_id]
	if t.busy or not t.can_act:
		return
	t.busy = true
	_refresh_ticket(ticket_id)
	var r: Dictionary = await bridge.post_service_timing(player_id, t.next_stage, ticket_id)
	if not tickets.has(ticket_id):
		return
	t.busy = false
	if r.ok:
		actions_done += 1
		response_ms_total += int(r.data.elapsed_since_previous_stage_ms)
		_update_stats()
		_set_status("Done: %s at %s." % [str(r.data.stage).replace("_", " "), t.table])
	elif r.status == 404:
		# The bridge no longer knows this ticket (it was restarted, or the ticket aged out).
		_set_status("The bridge lost ticket %s (restarted?). Dropped it." % t.table)
		_remove_ticket(ticket_id)
		return
	else:
		_set_status("Couldn't advance %s: %s" % [t.table, r.error])
	# Take the bridge's word for what happens next, rather than guessing locally.
	await _refresh_tickets()


# ---------------------------------------------------------------- shift report

func _show_report() -> void:
	report_label.text = "Building your shift report..."
	report_label.visible = true
	var url := "%s/api/comparison?source_id=%s&since=%s" % [dashboard_url, source_id.uri_encode(), shift_started.uri_encode()]
	var r: Dictionary = await bridge.request_json(HTTPClient.METHOD_GET, url)
	if not r.ok or not (r.data is Dictionary):
		report_label.text = "Shift report unavailable (dashboard at %s: %s)." % [dashboard_url, r.error]
		return
	report_label.text = format_report(r.data)


static func _secs(ms: Variant) -> String:
	return "-" if ms == null else "%.1fs" % (float(ms) / 1000.0)


static func _who(label: String, s: Dictionary) -> String:
	if int(s.n) == 0:
		return "%s  no actions yet" % label
	return "%s  %d action(s)   median %s   slowest 10%% over %s" % [label, int(s.n), _secs(s.median_ms), _secs(s.p90_ms)]


static func format_report(d: Dictionary) -> String:
	var sc: Dictionary = d.same_clock
	var lines: PackedStringArray = ["Shift report", ""]
	lines.append("You against the crew, same shift, same clock (how long a ticket waited for whoever handled it):")
	lines.append("  " + _who("You: ", sc.player))
	lines.append("  " + _who("Crew:", sc.crew))
	var ratio: Variant = sc.player_to_crew_median_ratio
	if ratio == null or float(ratio) <= 0.0:
		lines.append("  Not enough on both sides yet to compare you with the crew.")
	elif absf(float(ratio) - 1.0) < 0.05:
		lines.append("  You responded about as fast as the crew.")
	elif float(ratio) < 1.0:
		lines.append("  You responded %.1fx faster than the crew." % (1.0 / float(ratio)))
	else:
		lines.append("  You responded %.1fx slower than the crew." % float(ratio))
	for row in sc.by_stage:
		if int(row.player.n) > 0 and int(row.crew.n) > 0:
			lines.append("  %s: you %s (%d), crew %s (%d)" % [str(row.stage).replace("_", " "), _secs(row.player.median_ms), int(row.player.n), _secs(row.crew.median_ms), int(row.crew.n)])

	var vs: Dictionary = d.vs_simulated
	lines.append("")
	if int(vs.interactive_tickets) == 0 or int(vs.simulated_tickets) == 0:
		lines.append("Game tickets against the simulated restaurant: not enough completed tickets on both sides yet.")
	else:
		lines.append("Game tickets (%d) against the simulated restaurant (%d, last %d h), median per stage:" % [int(vs.interactive_tickets), int(vs.simulated_tickets), int(d.scope.reference_hours)])
		for m in vs.metrics:
			var beats: Variant = m.interactive_median_faster_than_pct_of_simulated
			lines.append("  %s: game %s, simulated %s%s" % [
				METRIC_LABELS.get(m.metric, m.metric), _secs(m.interactive.median_ms), _secs(m.simulated.median_ms),
				"" if beats == null else "  (game median beats %d%% of simulated tickets)" % int(round(float(beats))),
			])
		lines.append("  The game and the simulators run on different clocks; read this as a comparison of pace, not a score.")
	return "\n".join(lines)


# ---------------------------------------------------------------- findings

func _refresh_findings() -> void:
	var r: Dictionary = await bridge.request_json(HTTPClient.METHOD_GET, dashboard_url + "/api/findings/narrated?limit=1")
	if not r.ok:
		findings_label.text = "Dashboard not reachable at %s." % dashboard_url
		return
	var rows: Array = r.data if r.data is Array else []
	if rows.is_empty():
		findings_label.text = "No findings yet. They appear once the pipeline has seen enough activity."
		return
	var f: Dictionary = rows[0]
	findings_label.text = "%s\n(%s, refutation %s)" % [
		f.get("narrative_text", ""),
		f.get("model_used", "?"),
		"passed" if f.get("refutation_passed", false) else "not passed",
	]
