from kafka_producer import generate_event, USER_POOL, EVENT_TYPES
import json

print("=== Kafka Producer Code Test ===")
print(f"User pool size: {len(USER_POOL)}")
print(f"Event types: {EVENT_TYPES}")
print()

errors = []
for i in range(20):
    ev = generate_event()
    etype = ev["event_type"]
    qty = ev["quantity"]
    pid = ev["product_id"]

    if "event_id" not in ev:
        errors.append(f"Event {i}: missing event_id")
    if "user_id" not in ev:
        errors.append(f"Event {i}: missing user_id")
    if "quantity" not in ev:
        errors.append(f"Event {i}: missing quantity key")

    if etype in ("page_view", "search"):
        if pid is not None:
            errors.append(f"Event {i}: product_id should be None for {etype}, got {pid}")
        if qty is not None:
            errors.append(f"Event {i}: quantity should be None for {etype}, got {qty}")
    else:
        if qty is None:
            errors.append(f"Event {i}: quantity should NOT be None for {etype}")

    print(f"  Event {i+1:02d}: type={etype:<16} qty={str(qty):<5} product={pid}")

print()
if errors:
    print("ERRORS FOUND:")
    for e in errors:
        print(" ", e)
else:
    print("ALL 20 EVENTS VALID - Producer code is correct!")
    print()
    # Show sample JSON
    sample = generate_event()
    print("Sample event JSON:")
    print(json.dumps(sample, indent=2, default=str))
