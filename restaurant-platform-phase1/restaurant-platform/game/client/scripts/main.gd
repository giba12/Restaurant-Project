extends Control
## Restaurant Shift -- single-screen prototype.
##
## Clock in at a station, tickets arrive, and you advance each one through
## the kitchen stages. Every action goes to game-bridge, which validates it
## and publishes the same events the simulators emit (tagged source_kind
## "player"). The bottom panel shows the latest LLM-narrated finding from the
## platform's dashboard API, closing the loop: your shift -> the pipeline ->
## a finding.
##
## The bridge is the authority on rules (stage order, sequencing); this script
## only presents state and reports what the bridge rejected.

const BridgeClient := preload("res://scripts/bridge_client.gd")

signal world_loaded

# Label for the action that moves a ticket INTO the given stage.
const STAGE_ACTIONS := {
	"cook_started": "Start cooking",
	"plated": "Plate it",
	"picked_up_by_server": "Server pickup",
	"delivered": "Deliver",
}
const PLAYABLE_STATIONS := ["station-grill", "station-saute", "station-salad", "station-expo"]
const ROLE := "line_cook"
const MAX_OPEN_TICKETS := 5
const TICKET_SPAWN_SECONDS := 8.0
const FINDINGS_POLL_SECONDS := 15.0
const WORLD_RETRY_SECONDS := 3.0
const SLOW_STAGE_SECONDS := 15.0

var bridge: BridgeClient
var dashboard_url := "http://127.0.0.1:8080"

var world: Dictionary = {}
var stages: Array = []
var player_id := ""
var station := ""
var on_shift := false
var tickets: Dictionary = {}  # ticket_id -> {stage, stage_started_ms, created_ms, busy, card, info, age, button}
var completed := 0
var total_ticket_ms := 0

var status_label: Label
var setup_panel: VBoxContainer
var shift_panel: VBoxContainer
var name_edit: LineEdit
var station_picker: OptionButton
var clock_in_button: Button
var clock_out_button: Button
var shift_header: Label
var stats_label: Label
var tickets_box: VBoxContainer
var findings_label: Label
var spawn_timer: Timer
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

	spawn_timer = _make_timer(TICKET_SPAWN_SECONDS, _spawn_ticket)
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
		age.add_theme_color_override("font_color", Color(0.95, 0.4, 0.35) if stage_secs > SLOW_STAGE_SECONDS else Color(0.85, 0.85, 0.85))


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

	# --- setup: name, station, clock in
	setup_panel = VBoxContainer.new()
	setup_panel.add_theme_constant_override("separation", 8)
	root_box.add_child(setup_panel)

	var name_row := HBoxContainer.new()
	name_row.add_theme_constant_override("separation", 8)
	var name_caption := Label.new()
	name_caption.text = "Your name"
	name_caption.custom_minimum_size.x = 90
	name_edit = LineEdit.new()
	name_edit.placeholder_text = "e.g. ana"
	name_edit.custom_minimum_size.x = 240
	name_row.add_child(name_caption)
	name_row.add_child(name_edit)
	setup_panel.add_child(name_row)

	var station_row := HBoxContainer.new()
	station_row.add_theme_constant_override("separation", 8)
	var station_caption := Label.new()
	station_caption.text = "Station"
	station_caption.custom_minimum_size.x = 90
	station_picker = OptionButton.new()
	station_picker.custom_minimum_size.x = 240
	for s in PLAYABLE_STATIONS:
		station_picker.add_item(s)
	station_row.add_child(station_caption)
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


func _set_status(text: String) -> void:
	status_label.text = text


func _update_stats() -> void:
	var avg := 0.0 if completed == 0 else float(total_ticket_ms) / completed / 1000.0
	stats_label.text = "Tickets delivered: %d    Average time per ticket: %.1fs" % [completed, avg]


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
	clock_in_button.disabled = on_shift
	_set_status("Connected. Pick a station and clock in.")
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
	if on_shift or world.is_empty():
		return
	player_id = _sanitize_player_id(name_edit.text)
	station = station_picker.get_item_text(station_picker.selected)
	clock_in_button.disabled = true
	var r: Dictionary = await bridge.post_staff_shift(player_id, ROLE, "clock_in")
	if not r.ok:
		clock_in_button.disabled = false
		_set_status("Couldn't clock in: %s" % r.error)
		return
	# Assign the station right away so the digital twin shows where they work.
	var r2: Dictionary = await bridge.post_staff_shift(player_id, ROLE, "station_reassign", station)
	if not r2.ok:
		_set_status("Clocked in, but couldn't take the station: %s" % r2.error)
	else:
		_set_status("On shift as player-%s at %s." % [player_id, station])
	on_shift = true
	completed = 0
	total_ticket_ms = 0
	_update_stats()
	shift_header.text = "player-%s  |  %s" % [player_id, station]
	setup_panel.visible = false
	shift_panel.visible = true
	await _spawn_ticket()
	spawn_timer.start()


func _on_clock_out_pressed() -> void:
	if not on_shift:
		return
	on_shift = false
	spawn_timer.stop()
	clock_out_button.disabled = true
	var r: Dictionary = await bridge.post_staff_shift(player_id, ROLE, "clock_out")
	clock_out_button.disabled = false
	for id in tickets.keys():
		_remove_ticket(id)
	shift_panel.visible = false
	setup_panel.visible = true
	clock_in_button.disabled = world.is_empty()
	if r.ok:
		_set_status("Shift over. You delivered %d ticket(s). Clock in again any time." % completed)
	else:
		_set_status("Clocked out locally, but the bridge said: %s" % r.error)


func _spawn_ticket() -> void:
	if not on_shift or tickets.size() >= MAX_OPEN_TICKETS:
		return
	var tables: Array = world.get("tables", [])
	var table: String = tables[randi() % tables.size()]
	var r: Dictionary = await bridge.post_service_timing(player_id, stages[0], null, table, station)
	if not r.ok:
		_set_status("New ticket failed: %s" % r.error)
		return
	if not on_shift:
		return
	_add_ticket(r.data.ticket_id, table, stages[0])


func _next_stage(stage: String) -> String:
	var idx := stages.find(stage)
	if idx == -1 or idx + 1 >= stages.size():
		return ""
	return stages[idx + 1]


func _add_ticket(ticket_id: String, table: String, stage: String) -> void:
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
	button.custom_minimum_size = Vector2(150, 34)
	row.add_child(info)
	row.add_child(age)
	row.add_child(button)
	tickets_box.add_child(card)

	var now := Time.get_ticks_msec()
	tickets[ticket_id] = {
		"stage": stage, "stage_started_ms": now, "created_ms": now, "busy": false,
		"card": card, "info": info, "age": age, "button": button, "table": table,
	}
	button.pressed.connect(_advance.bind(ticket_id))
	_refresh_ticket(ticket_id)


func _refresh_ticket(ticket_id: String) -> void:
	var t: Dictionary = tickets[ticket_id]
	var info: Label = t.info
	var button: Button = t.button
	info.text = "%s    now: %s" % [t.table, str(t.stage).replace("_", " ")]
	var nxt := _next_stage(t.stage)
	button.text = STAGE_ACTIONS.get(nxt, nxt)
	button.disabled = t.busy


func _remove_ticket(ticket_id: String) -> void:
	if not tickets.has(ticket_id):
		return
	var card: Control = tickets[ticket_id].card
	card.queue_free()
	tickets.erase(ticket_id)


func _advance(ticket_id: String) -> void:
	if not tickets.has(ticket_id) or tickets[ticket_id].busy:
		return
	var t: Dictionary = tickets[ticket_id]
	var nxt := _next_stage(t.stage)
	if nxt == "":
		return
	t.busy = true
	_refresh_ticket(ticket_id)
	var r: Dictionary = await bridge.post_service_timing(player_id, nxt, ticket_id)
	if not tickets.has(ticket_id):
		return
	t.busy = false
	if r.ok:
		t.stage = nxt
		t.stage_started_ms = Time.get_ticks_msec()
		if nxt == stages[stages.size() - 1]:
			completed += 1
			total_ticket_ms += Time.get_ticks_msec() - int(t.created_ms)
			_remove_ticket(ticket_id)
			_update_stats()
			return
		_refresh_ticket(ticket_id)
	elif r.status == 404:
		# The bridge no longer knows this ticket (it was restarted). Drop it.
		_set_status("The bridge lost ticket %s (restarted?). Dropped it." % t.table)
		_remove_ticket(ticket_id)
	else:
		_set_status("Couldn't advance %s: %s" % [t.table, r.error])
		_refresh_ticket(ticket_id)


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
