extends SceneTree
## Headless integration test against a LIVE game-bridge (docker compose up):
##
##   godot4 --headless --path game/client -s res://tests/smoke_test.gd
##
## Exits non-zero on any failure. Uses the fixed player id "smoke-test", so
## re-runs reuse one staff row in the digital twin instead of adding one per run.

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

	# Start from a known state regardless of how a previous run ended.
	await bridge.post_staff_shift("smoke-test", "line_cook", "clock_out")

	var r: Dictionary = await bridge.post_staff_shift("smoke-test", "line_cook", "clock_in")
	check(r.ok and r.data.source_kind == "player", "clock_in accepted, tagged player")
	r = await bridge.post_staff_shift("smoke-test", "line_cook", "clock_in")
	check(not r.ok and r.status == 409, "second clock_in rejected with 409 (%s)" % r.error)

	r = await bridge.post_service_timing("smoke-test", "order_fired", null, "table-01", "station-grill")
	check(r.ok, "order_fired accepted")
	var ticket_id: String = r.data.ticket_id
	r = await bridge.post_service_timing("smoke-test", "plated", ticket_id)
	check(not r.ok and r.status == 409, "skipping a stage rejected with 409 (%s)" % r.error)
	r = await bridge.post_service_timing("smoke-test", "order_fired", null, "table-99", "station-grill")
	check(not r.ok and r.status == 422, "unknown table rejected with 422")
	for stage in stages.slice(1):
		r = await bridge.post_service_timing("smoke-test", stage, ticket_id)
		check(r.ok and r.data.elapsed_since_previous_stage_ms != null, "%s accepted with elapsed" % stage)
	r = await bridge.post_service_timing("smoke-test", "cook_started", ticket_id)
	check(not r.ok and r.status == 404, "delivered ticket is closed (404)")
	r = await bridge.post_staff_shift("smoke-test", "line_cook", "clock_out")
	check(r.ok, "clock_out accepted")

	print("== main scene, driven through its own handlers ==")
	var main: Control = load("res://scenes/main.tscn").instantiate()
	root.add_child(main)
	if main.world.is_empty():
		await main.world_loaded
	check(main.stages.size() == 5, "main scene loaded the world from the bridge")

	main.name_edit.text = "Smoke Test"
	await main._on_clock_in_pressed()
	check(main.on_shift and main.player_id == "smoke-test", "clock in as sanitized 'smoke-test'")
	check(main.tickets.size() == 1, "a first ticket arrives immediately")
	var id: String = main.tickets.keys()[0]
	for i in 4:
		await main._advance(id)
	check(main.tickets.is_empty() and main.completed == 1, "advancing four times delivers the ticket")
	await main._on_clock_out_pressed()
	check(not main.on_shift and main.setup_panel.visible, "clock out returns to setup")

	print("failures: ", failures)
	quit(1 if failures > 0 else 0)
