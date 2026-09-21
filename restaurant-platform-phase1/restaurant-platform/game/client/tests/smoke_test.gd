extends SceneTree
## Headless integration test against a LIVE game-bridge (see game/README.md):
##
##   godot4 --headless --path game/client -s res://tests/smoke_test.gd
##
## Exits non-zero on any failure. Uses the fixed player id "smoke-test", so
## re-runs reuse one staff row in the digital twin instead of adding one per run.
## Takes up to ~1 minute: it waits for the bridge's crew and for a new ticket to arrive.

const BridgeClient := preload("res://scripts/bridge_client.gd")

var failures := 0


func check(condition: bool, label: String) -> void:
	if condition:
		print("  ok    ", label)
	else:
		failures += 1
		print("  FAIL  ", label)


func _initialize() -> void:
	_run.call_deferred()


func _wait(seconds: float) -> void:
	await create_timer(seconds).timeout


func _run() -> void:
	var bridge := BridgeClient.new()
	root.add_child(bridge)
	var env := OS.get_environment("BRIDGE_URL")
	if env != "":
		bridge.base_url = env

	print("== client ==")
	var world: Dictionary = await bridge.get_world()
	check(world.ok, "GET /api/world reachable (%s)" % bridge.base_url)
	if not world.ok:
		print("  ", world.error)
		quit(1)
		return
	var stages: Array = world.data.stages
	check(stages.size() == 5 and stages[0] == "order_fired", "world lists the five stages")
	check(world.data.playable_roles.has("line_cook") and world.data.playable_roles.has("server"), "world lists the playable roles")
	check(world.data.playable_roles.line_cook == ["cook_started", "plated"], "a line cook owns cook_started and plated")

	# Start from a known state regardless of how a previous run ended.
	await bridge.post_staff_shift("smoke-test", "line_cook", "clock_out")
	await bridge.post_staff_shift("smoke-test", "server", "clock_out")

	var r: Dictionary = await bridge.post_staff_shift("smoke-test", "server", "clock_in")
	check(r.ok and r.data.source_kind == "player", "clock_in as server accepted, tagged player")
	r = await bridge.post_staff_shift("smoke-test", "server", "clock_in")
	check(not r.ok and r.status == 409, "second clock_in rejected with 409 (%s)" % r.error)
	r = await bridge.post_service_timing("smoke-test", "order_fired", null, "table-01", "station-grill")
	check(r.ok, "a clocked-in player can fire a ticket")
	var server_ticket: String = r.data.ticket_id
	r = await bridge.post_service_timing("smoke-test", "cook_started", server_ticket)
	check(not r.ok and r.status == 403, "a server may not cook (403: %s)" % r.error)
	r = await bridge.post_staff_shift("smoke-test", "server", "clock_out")
	check(r.ok, "clock_out as server accepted")

	r = await bridge.post_staff_shift("smoke-test", "line_cook", "clock_in")
	check(r.ok, "clock_in as line_cook accepted")
	r = await bridge.post_staff_shift("smoke-test", "line_cook", "station_reassign", "station-saute")
	check(r.ok, "took station-saute")
	r = await bridge.post_service_timing("smoke-test", "order_fired", null, "table-02", "station-saute")
	check(r.ok, "fired a ticket at my station")
	var ticket_id: String = r.data.ticket_id
	r = await bridge.post_service_timing("smoke-test", "plated", ticket_id)
	check(not r.ok and r.status == 409, "skipping a stage rejected with 409 (%s)" % r.error)
	r = await bridge.post_service_timing("smoke-test", "order_fired", null, "table-99", "station-saute")
	check(not r.ok and r.status == 422, "unknown table rejected with 422")
	r = await bridge.post_service_timing("smoke-test", "cook_started", ticket_id)
	check(r.ok and r.data.elapsed_since_previous_stage_ms != null, "cook_started accepted with elapsed")
	r = await bridge.post_service_timing("smoke-test", "plated", ticket_id)
	check(r.ok, "plated accepted")
	r = await bridge.post_service_timing("smoke-test", "picked_up_by_server", ticket_id)
	check(not r.ok and r.status == 403, "a cook may not pick up (403: %s)" % r.error)

	r = await bridge.get_tickets("smoke-test")
	var mine := {}
	for row in r.data:
		mine[row.ticket_id] = row
	check(mine.has(ticket_id) and mine[ticket_id].waiting_on == "crew", "the plated ticket is waiting on the crew")

	print("   (waiting for the crew to pick up and deliver it; up to 40 s)")
	var gone := false
	for i in 40:
		await _wait(1.0)
		r = await bridge.get_tickets("smoke-test")
		gone = true
		for row in r.data:
			if row.ticket_id == ticket_id:
				gone = false
		if gone:
			break
	check(gone, "the crew finished the ticket (pickup and delivery)")
	r = await bridge.post_staff_shift("smoke-test", "line_cook", "clock_out")
	check(r.ok, "clock_out accepted")

	print("== main scene, driven through its own handlers ==")
	var main: Control = load("res://scenes/main.tscn").instantiate()
	root.add_child(main)
	if main.world.is_empty():
		await main.world_loaded
	check(main.stages.size() == 5, "main scene loaded the world from the bridge")
	check(main.role_picker.item_count == 2 and main.station_picker.item_count == 4, "role and station pickers filled from the world")

	main.name_edit.text = "Smoke Test"
	main.role_picker.select(main.role_picker.get_item_index(0))
	var cook_index := -1
	for i in main.role_picker.item_count:
		if main.role_picker.get_item_text(i) == "line_cook":
			cook_index = i
	main.role_picker.select(cook_index)
	main._on_role_selected(cook_index)
	check(main.station_row.visible, "a line cook is asked for a station")
	await main._on_clock_in_pressed()
	check(main.on_shift and main.player_id == "smoke-test" and main.role == "line_cook", "clock in as sanitized 'smoke-test', line cook")

	# Tickets left over from the API section above may already be mid-way (the crew
	# cooks at a station nobody is working); wait for a fresh one that needs cooking.
	var actionable := ""
	for i in 25:
		await _wait(1.0)
		await main._refresh_tickets()
		for id in main.tickets:
			if main.tickets[id].can_act and main.tickets[id].next_stage == "cook_started":
				actionable = id
		if actionable != "":
			break
	check(actionable != "", "a fresh ticket arrives at my station on its own")
	if actionable != "":
		var t: Dictionary = main.tickets[actionable]
		check(t.next_stage == "cook_started" and not t.button.disabled, "its button is live: %s" % t.button.text)
		await main._advance(actionable)
		await main._advance(actionable)
		check(main.actions_done == 2, "two actions counted (cook, plate)")
		t = main.tickets[actionable]
		check(t.waiting_on == "crew" and t.button.disabled, "after plating, the card says '%s'" % t.button.text)
	await main._on_clock_out_pressed()
	check(not main.on_shift and main.setup_panel.visible, "clock out returns to setup")

	print("failures: ", failures)
	quit(1 if failures > 0 else 0)
