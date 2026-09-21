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
const TICKET_POLL_SECONDS := 1.5
const FINDINGS_POLL_SECONDS := 15.0
const WORLD_RETRY_SECONDS := 3.0
const SLOW_STAGE_SECONDS := 15.0

var bridge: BridgeClient
var dashboard_url := "http://127.0.0.1:8080"

var world: Dictionary = {}
var stages: Array = []
var player_id := ""
var role := ""
var station := ""
var on_shift := false
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
var tickets_box: VBoxContainer
var findings_label: Label
var poll_timer: Timer
var findings_timer: Timer
var retry_timer: Timer


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
	findings_timer = _make_timer(FINDINGS_POLL_SECONDS, _refresh_findings)
	retry_timer = _make_timer(WORLD_RETRY_SECONDS, _load_world)
	findings_timer.start()
	_refresh_findings()
	_load_world()


func _notification(what: int) -> void:
	if what == NOTIFICATION_WM_CLOSE_REQUEST:
		if on_shift:
			await _on_clock_out_pressed()
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


func _on_role_selected(_index: int) -> void:
	station_row.visible = role_picker.get_item_text(role_picker.selected) == LINE_COOK


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
		for s in world.get("playable_stations", []):
			station_picker.add_item(s)
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
	if on_shift or world.is_empty() or role_picker.item_count == 0:
		return
	player_id = _sanitize_player_id(name_edit.text)
	role = role_picker.get_item_text(role_picker.selected)
	station = station_picker.get_item_text(station_picker.selected) if role == LINE_COOK else ""
	clock_in_button.disabled = true
	var r: Dictionary = await bridge.post_staff_shift(player_id, role, "clock_in")
	if not r.ok:
		clock_in_button.disabled = false
		_set_status("Couldn't clock in: %s" % r.error)
		return
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


func _on_clock_out_pressed() -> void:
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
