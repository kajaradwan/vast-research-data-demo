"""Stand-in for the DataEngine runtime: consume the element trigger's CloudEvents from Kafka and call handler().

Runs as uid 7101 (rle-svc). Reads only rle-svc's own key file. Usage:
  runner.py [--group G] [--idle-exit SECONDS] [--from-beginning]
With --idle-exit it stops after that many seconds without a new event (used by demo.py); without it, it runs forever.
"""
import argparse, json, os, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from confluent_kafka import Consumer
import rle
from main import handler, ensure_tables

ap = argparse.ArgumentParser()
ap.add_argument("--group", default="rle-transform")
ap.add_argument("--idle-exit", type=float, default=0)
ap.add_argument("--from-beginning", action="store_true")
a = ap.parse_args()

session = rle.db("rle-svc")
ensure_tables(session)
c = Consumer({"bootstrap.servers": rle.KAFKA, "group.id": a.group, "enable.auto.commit": False,
              "auto.offset.reset": "earliest" if a.from_beginning else "latest", "max.poll.interval.ms": 3_600_000})
assigned = []
c.subscribe(["rle-landing-events"], on_assign=lambda cons, parts: assigned.extend(parts))
while not assigned: c.poll(0.5)        # only report ready once partitions are assigned, so no event is missed
print(json.dumps({"runner": "up", "uid": os.getuid(), "group": a.group, "partitions": len(assigned)}), flush=True)
last = time.time()
while True:
    m = c.poll(1.0)
    if m is None or m.error():
        if a.idle_exit and time.time() - last > a.idle_exit: break
        continue
    h = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in (m.headers() or [])}
    event = {"id": h.get("ce_id"), "type": h.get("ce_type"), "source": h.get("ce_source"),
             "time": h.get("ce_time"), "elementpath": h.get("ce_elementpath"), "data": m.value().decode()}
    out = handler({"session": session}, event)
    print(json.dumps({"event": event["id"], "path": event["elementpath"], **out}), flush=True)
    c.commit(m)
    last = time.time()
c.close()
