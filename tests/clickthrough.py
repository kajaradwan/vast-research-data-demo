"""Browser click-through of the live deck (Playwright, Chromium, 1920x1080): every slide, every button.

  .venv/bin/python tests/clickthrough.py "<deck url with ?k=token>" [--skip teardown] [--only id,id] [--out DIR]

For each slide: screenshot. For each button on it, in order: click (confirm dialogs accepted), wait for the button to
turn green (exit 0) or orange (non-zero), record the time and the last lines of output, screenshot again. Also checks
that a request without the token gets 403, and that no data-network address or internal host name is visible on any
slide. Writes <out>/results.json and <out>/results.md (the table that goes in the README).
"""
import argparse, json, os, re, sys, time, urllib.request, urllib.error
from playwright.sync_api import sync_playwright

ap = argparse.ArgumentParser()
ap.add_argument("url")
ap.add_argument("--skip", default="teardown")
ap.add_argument("--only", default="")
ap.add_argument("--out", default=os.path.join("evidence", "clickthrough-" + time.strftime("%Y%m%d-%H%M")))
ap.add_argument("--timeout", type=int, default=900)
a = ap.parse_args()
os.makedirs(a.out, exist_ok=True)
skip = set(filter(None, a.skip.split(","))); only = set(filter(None, a.only.split(",")))
base = a.url.split("?")[0].rstrip("/")
LEAK = re.compile(r"\b(?:10|172)\.\d{1,3}\.\d{1,3}\.\d{1,3}\b|selab|se-var-|vastdata@")

res = {"url": base, "started": time.strftime("%Y-%m-%d %H:%M:%S %Z"), "checks": {}, "steps": [], "slides": []}
try:
    urllib.request.urlopen(base + "/api/steps", timeout=10); res["checks"]["no_token"] = "NOT REFUSED"
except urllib.error.HTTPError as e:
    res["checks"]["no_token"] = f"refused {e.code}"
try:
    urllib.request.urlopen(urllib.request.Request(base + "/api/run/status", method="POST"), timeout=10)
    res["checks"]["no_token_run"] = "NOT REFUSED"
except urllib.error.HTTPError as e:
    res["checks"]["no_token_run"] = f"refused {e.code}"

with sync_playwright() as p:
    b = p.chromium.launch()
    pg = b.new_page(viewport={"width": 1920, "height": 1080})
    pg.on("dialog", lambda d: d.accept())
    pg.goto(a.url); pg.wait_for_load_state("networkidle")
    n = pg.evaluate("document.querySelectorAll('.slide').length")
    for i in range(n):
        pg.goto(f"{base}/#{i + 1}"); pg.reload(); pg.wait_for_timeout(2500)
        slide = pg.locator(".slide.on")
        title = slide.locator("h1, h2").first.inner_text().replace("\n", " ") if slide.locator("h1, h2").count() else ""
        text = slide.inner_text()
        leaks = sorted(set(LEAK.findall(text)))
        shot = os.path.join(a.out, f"slide{i + 1:02d}.png"); pg.screenshot(path=shot)
        res["slides"].append({"n": i + 1, "title": title, "leaks": leaks, "shot": os.path.basename(shot)})
        print(f"slide {i + 1:2d} {title[:60]:60} leaks={leaks or 'none'}", flush=True)
        for btn in slide.locator("button.step").all():
            sid = btn.get_attribute("data-step")
            if sid in skip or (only and sid not in only):
                res["steps"].append({"slide": i + 1, "step": sid, "result": "skipped"}); continue
            t = time.time(); btn.click()
            try:
                pg.wait_for_function(f"document.querySelector('.slide.on button[data-step=\"{sid}\"]').className.match(/\\b(ok|bad)\\b/)",
                                     timeout=a.timeout * 1000)
                cls = btn.get_attribute("class")
                ok = " ok" in f" {cls}"
            except Exception:
                ok = None
            dt = time.time() - t
            pg.wait_for_timeout(3500)                       # let the panel poll once more after the run
            tail = slide.locator("[data-term]").inner_text().strip().splitlines()[-3:]
            shot = os.path.join(a.out, f"slide{i + 1:02d}-{sid}.png"); pg.screenshot(path=shot)
            leaks = sorted(set(LEAK.findall(slide.inner_text())))
            r = {"slide": i + 1, "step": sid, "result": "exit 0" if ok else ("TIMEOUT" if ok is None else "NON-ZERO"),
                 "seconds": round(dt, 1), "tail": tail, "leaks": leaks, "shot": os.path.basename(shot)}
            res["steps"].append(r)
            print(f"   {sid:12} {r['result']:9} {dt:6.1f}s  {(tail or [''])[-2] if len(tail) > 1 else ''}"[:160], flush=True)
    b.close()

res["finished"] = time.strftime("%Y-%m-%d %H:%M:%S %Z")
json.dump(res, open(os.path.join(a.out, "results.json"), "w"), indent=1)
ran = [s for s in res["steps"] if s["result"] != "skipped"]
bad = [s for s in ran if s["result"] != "exit 0"]
leaky = [s for s in res["slides"] + ran if s.get("leaks")]
with open(os.path.join(a.out, "results.md"), "w") as f:
    f.write(f"Click-through {res['started']} → {res['finished']}, Chromium 1920×1080 (Playwright)\n\n")
    f.write(f"- No token: GET {res['checks']['no_token']}, POST run {res['checks']['no_token_run']}\n")
    f.write(f"- Slides: {len(res['slides'])}; buttons run: {len(ran)}, passed: {len(ran) - len(bad)}; skipped: "
            f"{', '.join(s['step'] for s in res['steps'] if s['result'] == 'skipped') or 'none'}\n")
    f.write(f"- Addresses or internal names visible: {'none' if not leaky else leaky}\n\n| Slide | Button | Result | Time (s) |\n|---|---|---|---|\n")
    for s in ran: f.write(f"| {s['slide']} | {s['step']} | {s['result']} | {s['seconds']} |\n")
print(open(os.path.join(a.out, "results.md")).read())
sys.exit(1 if bad or leaky or "NOT" in json.dumps(res["checks"]) else 0)
