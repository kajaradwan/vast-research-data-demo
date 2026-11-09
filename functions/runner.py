"""Stand-in for the DataEngine runtime: consume a trigger's CloudEvents from Kafka and call that function's handler().

  runner.py genomics|hep [--idle-exit S] [--from-beginning] [--group G]

The trigger, the CloudEvents and the broker are DataEngine's own. Only the function runtime is stood in, because no
Kubernetes compute cluster is linked to the tenant: with one linked, the same handler(ctx, event) is built with
`vastde functions build/create` and wired to the trigger by a pipeline. Runs as rlu-svc and reads only its own key.
"""
import argparse, json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
import rlu  # noqa: E402
from env import KAFKA, TOPICS, P  # noqa: E402
from confluent_kafka import Consumer  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("function", choices=["genomics", "hep"])
ap.add_argument("--group")
ap.add_argument("--idle-exit", type=float, default=0)
ap.add_argument("--from-beginning", action="store_true")
a = ap.parse_args()

if a.function == "genomics":
    sys.path.insert(0, os.path.join(HERE, "landing"))
    from main import handler, ensure_tables  # noqa: E402
else:
    sys.path.insert(0, os.path.join(HERE, "adfilter"))
    from handler import handler, ensure_tables  # noqa: E402

session = rlu.db(f"{P}-svc")
ensure_tables(session)
c = Consumer({"bootstrap.servers": KAFKA, "group.id": a.group or f"{P}-{a.function}-fn", "enable.auto.commit": False,
              "auto.offset.reset": "earliest" if a.from_beginning else "latest", "max.poll.interval.ms": 3_600_000})
assigned = []
c.subscribe([TOPICS[a.function]], on_assign=lambda cons, parts: assigned.extend(parts))
while not assigned:
    c.poll(0.5)        # report ready only once partitions are assigned, so no event is missed
print(json.dumps({"runner": a.function, "uid": os.getuid(), "topic": TOPICS[a.function], "partitions": len(assigned)}), flush=True)
last = time.time()
while True:
    m = c.poll(1.0)
    if m is None or m.error():
        if a.idle_exit and time.time() - last > a.idle_exit: break
        continue
    h = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in (m.headers() or [])}
    event = {"id": h.get("ce_id"), "type": h.get("ce_type"), "source": h.get("ce_source"), "time": h.get("ce_time"),
             "elementpath": h.get("ce_elementpath"), "data": m.value().decode()}
    out = handler({"session": session}, event)
    print(json.dumps({"event": event["id"], "path": event["elementpath"], **out}), flush=True)
    c.commit(m)
    last = time.time()
c.close()
