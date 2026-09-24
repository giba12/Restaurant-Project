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


func _a_free_table(bridge_client: BridgeClient, tables: Array) -> String:
	# The live dining room fires its first ticket the instant anyone is staffed at all (its
	# spawn cooldown starts already elapsed), and every ticket anywhere -- staff, guest, dining
	# room -- now holds its table exclusively. A hardcoded table is therefore a real, if
	# infrequent, collision risk throughout this whole test; ask the board what is actually
	# free right now instead of assuming one.
	var used := {}
	for row in (await bridge_client.request_json(HTTPClient.METHOD_GET, bridge_client.base_url + "/api/tickets")).data:
		used[row.table_id] = true
	for t in tables:
		if not used.has(t):
			return t
	return tables[0]  # every table is somehow busy; fall back and let the caller's check fail visibly


func _select_a_free_table(main_scene: Control) -> void:
	var t := await _a_free_table(main_scene.bridge, main_scene.world.tables)
	for i in main_scene.table_picker.item_count:
		if main_scene.table_picker.get_item_text(i) == t:
			main_scene.table_picker.select(i)
			return


func _test_report_format() -> void:
	var main_script: GDScript = load("res://scripts/main.gd")
	var stat := func(n: int, med: Variant, p90: Variant) -> Dictionary: return {"n": n, "median_ms": med, "p90_ms": p90}
	var data := {
		"scope": {"reference_hours": 24},
		"same_clock": {
			"player": stat.call(3, 4000, 4800), "crew": stat.call(3, 10000, 11600),
			"player_to_crew_median_ratio": 0.4,
			"by_stage": [
				{"stage": "cook_started", "player": stat.call(2, 3000, 5000), "crew": stat.call(1, 9000, 9000)},
				{"stage": "plated", "player": stat.call(1, 4000, 4000), "crew": stat.call(0, null, null)},
			],
		},
		"vs_simulated": {
			"interactive_tickets": 89, "simulated_tickets": 1900,
			"metrics": [{"metric": "pickup_delay_ms", "interactive": stat.call(89, 8900, 12000), "simulated": stat.call(1900, 20700, 64000),
				"interactive_median_faster_than_pct_of_simulated": 69.7}],
		},
	}
	var text: String = main_script.format_report(data)
	check(text.begins_with("Shift report"), "report has a title")
	check(text.contains("You:   3 action(s)   median 4.0s   slowest 10% over 4.8s"), "player line: n, median, p90")
	check(text.contains("Crew:  3 action(s)   median 10.0s"), "crew line")
	check(text.contains("2.5x faster than the crew"), "ratio 0.4 reads as 2.5x faster")
	check(text.contains("cook started: you 3.0s (2), crew 9.0s (1)"), "a stage both did is shown side by side")
	check(not text.contains("plated: you"), "a stage only one side did is left out")
	check(text.contains("Pickup delay: game 8.9s, simulated 20.7s  (game median beats 70% of simulated tickets)"), "vs-simulated line with the percentage")
	check(text.contains("Game tickets (89) against the simulated restaurant (1900, last 24 h)"), "vs-simulated header with counts")

	data.same_clock.player_to_crew_median_ratio = 1.5
	check(main_script.format_report(data).contains("1.5x slower than the crew"), "ratio 1.5 reads as 1.5x slower")
	data.same_clock.player_to_crew_median_ratio = 1.02
	check(main_script.format_report(data).contains("about as fast as the crew"), "ratio near 1 reads as about as fast")
	data.same_clock.player_to_crew_median_ratio = null
	data.same_clock.crew = stat.call(0, null, null)
	data.vs_simulated.interactive_tickets = 0
	var thin: String = main_script.format_report(data)
	check(thin.contains("Not enough on both sides yet"), "no crew data: says so instead of a ratio")
	check(thin.contains("Crew:  no actions yet") and thin.contains("not enough completed tickets"), "empty sides are stated plainly")


func _run() -> void:
	var bridge := BridgeClient.new()
	root.add_child(bridge)
	var env := OS.get_environment("BRIDGE_URL")
	if env != "":
		bridge.base_url = env
	bridge.api_key = OS.get_environment("BRIDGE_API_KEY")  # this section only ever calls the bridge, not the dashboard

	print("== shift report formatting (canned data, no network) ==")
	_test_report_format()

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
	check(world.data.playable_roles.line_cook == ["cook_started"], "a line cook owns cook_started")
	check(world.data.playable_roles.expo == ["plated"], "an expo owns plated")
	check(not world.data.playable_stations.has("station-expo"), "station-expo is not a cooking station")

	# Start from a known state regardless of how a previous run ended.
	await bridge.post_staff_shift("smoke-test", "line_cook", "clock_out")
	await bridge.post_staff_shift("smoke-test", "server", "clock_out")

	var r: Dictionary = await bridge.post_staff_shift("smoke-test", "server", "clock_in")
	check(r.ok and r.data.source_kind == "player", "clock_in as server accepted, tagged player")
	r = await bridge.post_staff_shift("smoke-test", "server", "clock_in")
	check(not r.ok and r.status == 409, "second clock_in rejected with 409 (%s)" % r.error)
	r = await bridge.post_service_timing("smoke-test", "order_fired", null, await _a_free_table(bridge, world.data.tables), "station-grill")
	check(r.ok, "a clocked-in player can fire a ticket (%s)" % r.error)
	var server_ticket: String = r.data.get("ticket_id", "")
	r = await bridge.post_service_timing("smoke-test", "cook_started", server_ticket)
	check(not r.ok and r.status == 403, "a server may not cook (403: %s)" % r.error)
	r = await bridge.post_staff_shift("smoke-test", "server", "clock_out")
	check(r.ok, "clock_out as server accepted")

	r = await bridge.post_staff_shift("smoke-test", "line_cook", "clock_in")
	check(r.ok, "clock_in as line_cook accepted")
	r = await bridge.post_staff_shift("smoke-test", "line_cook", "station_reassign", "station-saute")
	check(r.ok, "took station-saute")
	r = await bridge.post_service_timing("smoke-test", "order_fired", null, await _a_free_table(bridge, world.data.tables), "station-saute")
	check(r.ok, "fired a ticket at my station (%s)" % r.error)
	var ticket_id: String = r.data.get("ticket_id", "")
	r = await bridge.post_service_timing("smoke-test", "plated", ticket_id)
	check(not r.ok and r.status == 409, "skipping a stage rejected with 409 (%s)" % r.error)
	r = await bridge.post_service_timing("smoke-test", "order_fired", null, "table-99", "station-saute")
	check(not r.ok and r.status == 422, "unknown table rejected with 422")
	r = await bridge.post_service_timing("smoke-test", "cook_started", ticket_id)
	check(r.ok and r.data.elapsed_since_previous_stage_ms != null, "cook_started accepted with elapsed")
	r = await bridge.post_service_timing("smoke-test", "plated", ticket_id)
	check(not r.ok and r.status == 403, "a cook may not plate (403: %s)" % r.error)
	r = await bridge.post_staff_shift("smoke-test", "line_cook", "clock_out")
	check(r.ok, "clock_out as line_cook accepted")

	r = await bridge.post_staff_shift("smoke-test", "expo", "clock_in")
	check(r.ok, "clock_in as expo accepted")
	r = await bridge.post_service_timing("smoke-test", "plated", ticket_id)
	check(r.ok, "plated accepted from expo")
	r = await bridge.post_service_timing("smoke-test", "picked_up_by_server", ticket_id)
	check(not r.ok and r.status == 403, "an expo may not pick up (403: %s)" % r.error)
	r = await bridge.post_staff_shift("smoke-test", "expo", "clock_out")
	check(r.ok, "clock_out as expo accepted")

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
	# already clocked out (as expo) above, so smoke-test is off shift here

	print("== guest (ordering, eating, paying) ==")
	check(world.data.has("menu") and world.data.menu.has("menu-burger-classic"), "world lists the menu")
	check(world.data.has("payment_methods") and world.data.payment_methods.has("card"), "world lists payment methods")

	r = await bridge.post_guest_order("smoke-test-guest", await _a_free_table(bridge, world.data.tables),
		[{"menu_item_id": "menu-burger-classic", "quantity": 2}, {"menu_item_id": "menu-drink-soda", "quantity": 1}])
	check(r.ok, "guest order accepted with no clock-in (%s)" % r.error)
	var expected_total: int = 2 * world.data.menu["menu-burger-classic"] + world.data.menu["menu-drink-soda"]
	check(r.data.get("total_amount_cents", -1) == expected_total, "order total matches the menu prices")
	check(r.data.get("delivered", true) == false, "not delivered yet")

	r = await bridge.post_guest_pay("smoke-test-guest", "cash")
	check(not r.ok and r.status == 409, "can't pay before delivery (409: %s)" % r.error)

	print("   (waiting for the crew to cook, plate, pick up and deliver the guest's order; up to 60 s)")
	var delivered := false
	for i in 60:
		await _wait(1.0)
		r = await bridge.get_guest_status("smoke-test-guest")
		if r.ok and r.data.delivered:
			delivered = true
			break
	check(delivered, "the guest's order was delivered with nobody staffed")

	r = await bridge.post_guest_pay("smoke-test-guest", "cash")
	check(r.ok, "pay accepted once delivered (%s)" % r.error)
	check(r.data.event_type == "POSTransactionEvent" and r.data.source_kind == "player", "a real, player-tagged POS transaction")
	check(r.data.payment_method == "cash" and r.data.total_amount_cents == expected_total, "the transaction matches the order")

	r = await bridge.get_guest_status("smoke-test-guest")
	check(not r.ok and r.status == 404, "the order is gone after paying (404: %s)" % r.error)

	print("== table exclusivity and leaving ==")
	# Start from a known state regardless of how a previous run ended (see the top of this
	# function): the section below deliberately leaves smoke-guest-a holding an order.
	await bridge.post_guest_leave("smoke-guest-a")
	await bridge.post_guest_leave("smoke-guest-b")
	var table_a: String = await _a_free_table(bridge, world.data.tables)
	r = await bridge.post_guest_order("smoke-guest-a", table_a, [{"menu_item_id": "menu-soup-of-day"}])
	check(r.ok, "first guest seats %s (%s)" % [table_a, r.error])
	r = await bridge.post_guest_order("smoke-guest-b", table_a, [{"menu_item_id": "menu-soup-of-day"}])
	check(not r.ok and r.status == 409, "a second guest can't use the same table (409: %s)" % r.error)
	r = await bridge.post_guest_leave("smoke-guest-a")
	check(r.ok and r.data.get("table_id", "") == table_a, "leaving reports the table")
	r = await bridge.get_guest_status("smoke-guest-a")
	check(not r.ok and r.status == 404, "the guest is gone right after leaving (404: %s)" % r.error)
	var table_b: String = await _a_free_table(bridge, world.data.tables)
	r = await bridge.post_guest_order("smoke-guest-a", table_b, [{"menu_item_id": "menu-soup-of-day"}])
	check(r.ok, "the same name can sit down again immediately, elsewhere (%s)" % r.error)
	r = await bridge.post_guest_order("smoke-guest-b", table_a, [{"menu_item_id": "menu-soup-of-day"}])
	check(not r.ok and r.status == 409, "%s is still occupied -- the kitchen is still cooking that ticket" % table_a)
	r = await bridge.post_guest_leave("smoke-guest-b")
	check(not r.ok and r.status == 404, "leaving with no open order is 404 (%s)" % r.error)

	print("== main scene, driven through its own handlers ==")
	var main: Control = load("res://scenes/main.tscn").instantiate()
	root.add_child(main)
	if main.world.is_empty():
		await main.world_loaded
	check(main.stages.size() == 5, "main scene loaded the world from the bridge")
	check(main.role_picker.item_count == 4 and main.station_picker.item_count == 3, "role and station pickers filled from the world")

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
		check(main.actions_done == 1, "one action counted (cook_started)")
		t = main.tickets[actionable]
		check(t.next_stage == "plated" and t.waiting_on == "crew" and t.button.disabled, "plating isn't mine: card says '%s'" % t.button.text)
	await _wait(4.0)  # let the events reach the database before the report reads them
	await main._on_clock_out_pressed()
	check(not main.on_shift and main.setup_panel.visible, "clock out returns to setup")
	check(main.report_label.visible and main.report_label.text.begins_with("Shift report"), "clock out shows a shift report")
	check(main.report_label.text.contains("You:   1 action(s)"), "the report counts my one action (%s)" % main.report_label.text.split("\n")[3].strip_edges())
	check(main.report_label.text.contains("Crew:"), "the report shows the crew alongside")

	print("== main scene, guest ordering ==")
	for i in main.role_picker.item_count:
		if main.role_picker.get_item_text(i) == "guest":
			main.role_picker.select(i)
	main._on_role_selected(main.role_picker.selected)
	check(main.table_row.visible and not main.station_row.visible, "guest is asked for a table, not a station")
	check(main.clock_in_button.text == "Sit down", "the button reads 'Sit down' for a guest")
	await _select_a_free_table(main)
	main.name_edit.text = "Smoke Guest"
	main._on_clock_in_pressed()  # no bridge call yet -- sitting down is local
	check(main.is_guest and main.guest_panel.visible and main.cart_section.visible, "sitting down shows the menu, not the bridge")
	check(main.place_order_button.disabled, "can't place an empty order")
	main.cart_spinboxes["menu-soup-of-day"].value = 1
	check(not main.place_order_button.disabled and main.cart_total_label.text.contains("1 item"), "picking an item enables ordering")
	await main._on_place_order_pressed()
	check(not main.cart_section.visible and main.status_section.visible, "placing the order switches to waiting")

	var guest_delivered := false
	for i in 60:
		await _wait(1.0)
		await main._refresh_guest_status()
		if main.guest_delivered:
			guest_delivered = true
			break
	check(guest_delivered, "the guest's own order arrives on its own")
	check(main.pay_row.visible, "the pay button appears once delivered")
	await main._on_pay_pressed()
	check(main.order_status_label.text.begins_with("Paid $"), "paying shows a receipt (%s)" % main.order_status_label.text)
	main._on_guest_leave_pressed()
	check(not main.is_guest and main.setup_panel.visible, "leaving returns to setup")

	print("== main scene, leaving before paying reaches the bridge ==")
	await _select_a_free_table(main)
	main._on_clock_in_pressed()  # still "guest" selected; sits at whichever table is free now
	main.cart_spinboxes["menu-drink-soda"].value = 1
	await main._on_place_order_pressed()
	check(main.status_section.visible and not main.guest_paid, "a second order placed, not yet paid")
	await main._on_guest_leave_pressed()
	r = await bridge.get_guest_status("smoke-guest")
	check(not r.ok and r.status == 404, "the client's Leave button told the bridge (404: %s)" % r.error)

	print("failures: ", failures)
	quit(1 if failures > 0 else 0)
