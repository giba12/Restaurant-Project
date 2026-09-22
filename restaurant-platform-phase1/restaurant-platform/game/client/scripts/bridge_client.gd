extends Node
## Thin async HTTP client for game-bridge (and the dashboard API's read endpoints).
##
## Every call returns a Dictionary: {ok: bool, status: int, data: Variant, error: String}.
## status is 0 when the request never got an HTTP response (bridge down, timeout).
## The bridge owns validation and state rules; this client only reports what it said.

var base_url: String = "http://127.0.0.1:8001"
var timeout_seconds: float = 5.0


func get_world() -> Dictionary:
	return await request_json(HTTPClient.METHOD_GET, base_url + "/api/world")


func post_staff_shift(player_id: String, role: String, shift_action: String, station_id: Variant = null) -> Dictionary:
	var body := {"player_id": player_id, "role": role, "shift_action": shift_action}
	if station_id != null:
		body["station_id"] = station_id
	return await request_json(HTTPClient.METHOD_POST, base_url + "/api/staff-shift", body)


func post_service_timing(player_id: String, stage: String, ticket_id: Variant = null, table_id: Variant = null, station_id: Variant = null) -> Dictionary:
	var body := {"player_id": player_id, "stage": stage}
	if ticket_id != null:
		body["ticket_id"] = ticket_id
	if table_id != null:
		body["table_id"] = table_id
	if station_id != null:
		body["station_id"] = station_id
	return await request_json(HTTPClient.METHOD_POST, base_url + "/api/service-timing", body)


func get_tickets(player_id: String) -> Dictionary:
	return await request_json(HTTPClient.METHOD_GET, base_url + "/api/tickets?player_id=" + player_id.uri_encode())


func post_guest_order(player_id: String, table_id: String, items: Array) -> Dictionary:
	return await request_json(HTTPClient.METHOD_POST, base_url + "/api/guest/order",
		{"player_id": player_id, "table_id": table_id, "items": items})


func get_guest_status(player_id: String) -> Dictionary:
	return await request_json(HTTPClient.METHOD_GET, base_url + "/api/guest/status?player_id=" + player_id.uri_encode())


func post_guest_pay(player_id: String, payment_method: String) -> Dictionary:
	return await request_json(HTTPClient.METHOD_POST, base_url + "/api/guest/pay",
		{"player_id": player_id, "payment_method": payment_method})


func post_guest_leave(player_id: String) -> Dictionary:
	return await request_json(HTTPClient.METHOD_POST, base_url + "/api/guest/leave?player_id=" + player_id.uri_encode())


func request_json(method: int, url: String, body: Variant = null) -> Dictionary:
	# One HTTPRequest node per call: a node handles a single request at a time,
	# and the UI can have several in flight (ticket advance + findings poll).
	var http := HTTPRequest.new()
	http.timeout = timeout_seconds
	add_child(http)
	var headers := PackedStringArray(["Content-Type: application/json"])
	var payload := "" if body == null else JSON.stringify(body)
	var err := http.request(url, headers, method, payload)
	if err != OK:
		http.queue_free()
		return {"ok": false, "status": 0, "data": null, "error": "request failed to start (%s)" % error_string(err)}
	var result: Array = await http.request_completed
	http.queue_free()
	var result_code: int = result[0]
	var status: int = result[1]
	var raw: PackedByteArray = result[3]
	if result_code != HTTPRequest.RESULT_SUCCESS:
		return {"ok": false, "status": 0, "data": null, "error": "network error (code %d)" % result_code}
	var parsed: Variant = JSON.parse_string(raw.get_string_from_utf8())
	var ok := status >= 200 and status < 300
	return {"ok": ok, "status": status, "data": parsed, "error": "" if ok else _describe_error(parsed, status)}


func _describe_error(parsed: Variant, status: int) -> String:
	# FastAPI errors are {"detail": "message"} or {"detail": ["field: message", ...]}.
	if parsed is Dictionary and parsed.has("detail"):
		var detail: Variant = parsed["detail"]
		if detail is Array:
			var parts: PackedStringArray = []
			for d in detail:
				parts.append(str(d.get("msg", d)) if d is Dictionary else str(d))
			return "; ".join(parts)
		return str(detail)
	return "HTTP %d" % status
