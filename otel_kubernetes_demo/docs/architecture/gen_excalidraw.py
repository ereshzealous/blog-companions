#!/usr/bin/env python3
"""
Generate clean, non-overlapping Excalidraw architecture diagrams for the
WearableHealth OpenTelemetry POC.

Follows the ccc-skills:excalidraw rules:
  - every labeled shape = shape (boundElements) + separate text (containerId)
  - elbow arrows: roughness 0, roundness null, elbowed true
  - arrow x,y = source edge point; points relative; width/height = bbox
  - no diamonds; staggered fan-in/out; bindings for clean attach
"""
import json, itertools, os

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---- palette (background, stroke) -----------------------------------------
PAL = {
    "go":      ("#D6E4F5", "#1971c2"),
    "py":      ("#ECE7FB", "#7048e8"),
    "java":    ("#FBE6D4", "#f08c00"),
    "node":    ("#D7F0E3", "#2f9e44"),
    "db":      ("#ECEFF1", "#546e7a"),
    "kafka":   ("#FFF3BF", "#fab005"),
    "ext":     ("#FFFFFF", "#90a4ae"),
    "agent":   ("#D7F0E3", "#2f9e44"),
    "gateway": ("#ffa8a8", "#c92a2a"),   # orchestrator/hub = coral
    "sidecar": ("#F8D7DA", "#e03131"),
    "metrics": ("#FBE6D4", "#f08c00"),
    "traces":  ("#D6E4F5", "#1971c2"),
    "logs":    ("#ECEFF1", "#90a4ae"),
    "grafana": ("#E5DBFF", "#7048e8"),
    "app":     ("#ECE7FB", "#7048e8"),
}

FONT = 2  # 1=hand-drawn(Virgil), 2=normal(Helvetica), 3=code(Cascadia)

_seed = itertools.count(1)

def _base(eid, etype, x, y, w, h, **extra):
    el = {
        "id": eid, "type": etype, "x": x, "y": y, "width": w, "height": h,
        "angle": 0, "strokeColor": "#1e1e1e", "backgroundColor": "transparent",
        "fillStyle": "solid", "strokeWidth": 2, "strokeStyle": "solid",
        "roughness": 1, "opacity": 100, "groupIds": [], "frameId": None,
        "roundness": None, "seed": next(_seed), "version": 1,
        "versionNonce": next(_seed), "isDeleted": False, "boundElements": None,
        "updated": 1, "link": None, "locked": False,
    }
    el.update(extra)
    return el

class Diagram:
    def __init__(self, title=None):
        self.els = []
        self.shapes = {}   # id -> (x,y,w,h)
        self.title = title

    def box(self, eid, x, y, w, h, label, pal, stroke_w=2, dashed=False, big=False):
        bg, st = PAL[pal]
        shape = _base(eid, "rectangle", x, y, w, h,
                      strokeColor=st, backgroundColor=bg, fillStyle="solid",
                      strokeWidth=3 if big else stroke_w,
                      strokeStyle="dashed" if dashed else "solid",
                      roughness=1, roundness={"type": 3},
                      boundElements=[{"type": "text", "id": eid + "-text"}])
        self.els.append(shape)
        self._text(eid, x, y, w, h, label)
        self.shapes[eid] = (x, y, w, h)

    def ellipse(self, eid, x, y, w, h, label, pal):
        bg, st = PAL[pal]
        shape = _base(eid, "ellipse", x, y, w, h,
                      strokeColor=st, backgroundColor=bg, fillStyle="solid",
                      roughness=1, roundness={"type": 2},
                      boundElements=[{"type": "text", "id": eid + "-text"}])
        self.els.append(shape)
        self._text(eid, x, y, w, h, label)
        self.shapes[eid] = (x, y, w, h)

    def group(self, eid, x, y, w, h, label, color="#868e96"):
        self.els.append(_base(eid, "rectangle", x, y, w, h,
                              strokeColor=color, backgroundColor="transparent",
                              strokeStyle="dashed", roughness=0, roundness=None))
        self.els.append(_base(eid + "-label", "text", x + 16, y + 10, len(label) * 9, 22,
                              text=label, fontSize=16, fontFamily=FONT,
                              textAlign="left", verticalAlign="top",
                              strokeColor=color, roughness=0,
                              containerId=None, originalText=label, lineHeight=1.25,
                              baseline=16))

    def _text(self, eid, x, y, w, h, label):
        lines = label.count("\n") + 1
        th = lines * 19  # ~fontSize 15 * lineHeight 1.25
        # exporter centers the text block on the element's y, so anchor at box center
        self.els.append(_base(eid + "-text", "text", x + 5, y + h / 2,
                              w - 10, th, text=label, fontSize=15, fontFamily=FONT,
                              textAlign="center", verticalAlign="middle",
                              strokeColor="#1e1e1e", roughness=0,
                              containerId=eid, originalText=label,
                              lineHeight=1.25, baseline=12))

    def _edge(self, eid, edge, frac=0.5):
        x, y, w, h = self.shapes[eid]
        if edge == "top":    return (x + w * frac, y)
        if edge == "bottom": return (x + w * frac, y + h)
        if edge == "left":   return (x, y + h * frac)
        if edge == "right":  return (x + w, y + h * frac)

    def arrow(self, src, sedge, dst, dedge, color="#495057", label=None,
              sfrac=0.5, dfrac=0.5, dashed=False, both=False):
        sx, sy = self._edge(src, sedge, sfrac)
        tx, ty = self._edge(dst, dedge, dfrac)
        dx, dy = tx - sx, ty - sy
        fa = "v" if sedge in ("top", "bottom") else "h"
        la = "v" if dedge in ("top", "bottom") else "h"
        if fa == "v" and la == "v":
            pts = [[0, 0], [0, dy]] if abs(dx) < 6 else \
                  [[0, 0], [0, dy / 2], [dx, dy / 2], [dx, dy]]
        elif fa == "h" and la == "h":
            pts = [[0, 0], [dx, 0]] if abs(dy) < 6 else \
                  [[0, 0], [dx / 2, 0], [dx / 2, dy], [dx, dy]]
        elif fa == "v" and la == "h":
            pts = [[0, 0], [0, dy], [dx, dy]]
        else:
            pts = [[0, 0], [dx, 0], [dx, dy]]
        w = max(abs(p[0]) for p in pts) or 1
        h = max(abs(p[1]) for p in pts) or 1
        aid = f"arr-{src}-{dst}-{next(_seed)}"
        # NOTE: no startBinding/endBinding and elbowed=False on purpose — bound
        # elbow arrows get auto-rerouted by excalidraw.com (looping, crossing
        # boxes). Unbound straight-segment polylines render exactly as routed.
        self.els.append(_base(aid, "arrow", sx, sy, w, h,
                              strokeColor=color, strokeWidth=2,
                              strokeStyle="dashed" if dashed else "solid",
                              roughness=0, roundness=None, points=pts,
                              elbowed=False, lastCommittedPoint=None,
                              startBinding=None, endBinding=None,
                              startArrowhead="arrow" if both else None,
                              endArrowhead="arrow"))
        if label:
            # place the label on the longest straight segment, nudged off the line
            best = max(range(len(pts) - 1),
                       key=lambda i: abs(pts[i + 1][0] - pts[i][0]) +
                                     abs(pts[i + 1][1] - pts[i][1]))
            p0, p1 = pts[best], pts[best + 1]
            vertical = abs(p1[1] - p0[1]) > abs(p1[0] - p0[0])
            mx = sx + (p0[0] + p1[0]) / 2 + (10 if vertical else -len(label) * 3.2)
            my = sy + (p0[1] + p1[1]) / 2 - 18
            self.els.append(_base(aid + "-lbl", "text", mx, my,
                                  len(label) * 7, 18, text=label, fontSize=12,
                                  fontFamily=FONT, textAlign="left",
                                  verticalAlign="middle", strokeColor=color,
                                  backgroundColor="transparent", roughness=0,
                                  containerId=None, originalText=label,
                                  lineHeight=1.25, baseline=12))

    def title_text(self, x, y, text, size=24):
        self.els.append(_base(f"title-{next(_seed)}", "text", x, y, len(text) * size * 0.55, size + 8,
                              text=text, fontSize=size, fontFamily=FONT,
                              textAlign="left", verticalAlign="top",
                              strokeColor="#1e1e1e", roughness=0,
                              containerId=None, originalText=text,
                              lineHeight=1.25, baseline=size))

    def write(self, fname):
        doc = {"type": "excalidraw", "version": 2,
               "source": "claude-code-excalidraw-skill", "elements": self.els,
               "appState": {"gridSize": 20, "viewBackgroundColor": "#ffffff"},
               "files": {}}
        path = os.path.join(OUT_DIR, fname)
        with open(path, "w") as f:
            json.dump(doc, f, indent=2)
        # validation
        ids = [e["id"] for e in self.els]
        dups = {i for i in ids if ids.count(i) > 1}
        assert not dups, f"duplicate ids: {dups}"
        for e in self.els:
            if e.get("boundElements"):
                for b in e["boundElements"]:
                    if b["type"] == "text":
                        assert any(x["id"] == b["id"] for x in self.els), b
        print(f"wrote {fname}: {len(self.els)} elements, {len(dups)} dup ids")


# ===========================================================================
# Diagram 1 — Sidecar vs DaemonSet (the core argument)
# ===========================================================================
def diagram_sidecar_vs_daemonset():
    d = Diagram()
    d.title_text(60, 24, "Sidecar vs DaemonSet — the same fleet, two collector topologies")

    # ---- LEFT: SIDECAR ----
    d.group("g-side", 70, 90, 430, 490, "SIDECAR  ·  one collector per pod", "#e03131")
    pods = [("p1", "a1", "s1", 150), ("p2", "a2", "s2", 280), ("p3", "a3", "s3", 410)]
    for pod, app, side, py in pods:
        d.box(pod, 100, py, 370, 100, "", "ext", dashed=True)  # pod boundary
        d.box(app, 120, py + 20, 150, 60, "app pod", "app")
        d.box(side, 300, py + 20, 150, 60, "otel\nsidecar", "sidecar")
        d.arrow(app, "right", side, "left", color="#e03131")
    d.title_text(120, 535, "3 pods  =  3 collectors", size=16)

    # ---- RIGHT: DAEMONSET ----
    d.group("g-ds", 560, 90, 470, 490, "DAEMONSET (agent)  ·  one collector per node", "#2f9e44")
    apps = [("d1", 160), ("d2", 290), ("d3", 420)]
    for app, py in apps:
        d.box(app, 590, py, 160, 64, "app pod", "app")
    d.box("agent", 840, 230, 170, 120, "otel agent\nDaemonSet", "agent", big=True)
    for i, (app, py) in enumerate(apps):
        d.arrow(app, "right", "agent", "left", color="#2f9e44",
                dfrac=0.25 + 0.25 * i)
    d.title_text(610, 535, "3 pods  =  1 collector", size=16)

    d.write("01-sidecar-vs-daemonset.excalidraw")


# ===========================================================================
# Diagram 2 — High-level architecture (service data flow)
# ===========================================================================
def diagram_highlevel():
    d = Diagram()
    d.title_text(60, 20, "WearableHealth — high-level service flow  (every solid arrow carries traceparent)")

    LX, LW = 360, 190
    cx = LX + LW / 2
    # left lane = streaming/write pipeline
    d.ellipse("wear", LX + 20, 70, 150, 60, "Wearable", "ext")
    d.box("dg",   LX, 170, LW, 70, "device-gateway\nGo · gRPC ingest", "go")
    d.box("kraw", LX + 15, 285, LW - 30, 50, "Kafka · vitals.raw", "kafka")
    d.box("sp",   LX, 380, LW, 70, "stream-processor\nPython · rollups", "py")
    d.box("kevt", LX + 15, 495, LW - 30, 50, "Kafka · vitals.events", "kafka")
    d.box("ad",   LX, 590, LW, 70, "anomaly-detector\nPython · sidecar", "py")
    d.box("kalr", LX + 15, 705, LW - 30, 50, "Kafka · alerts", "kafka")
    d.box("lg",   LX, 800, LW, 70, "live-gateway\nNode.js · WebSocket", "node")
    d.ellipse("mob", LX + 20, 915, 150, 60, "Mobile app", "ext")

    # right lane = data + read services (each arrow hits a distinct Postgres edge)
    d.box("pg",   760, 360, 190, 100, "Postgres", "db", big=True)
    d.box("iw",   1030, 375, 190, 70, "insight-worker\nPython · CronJob", "py")
    d.box("ha",   760, 630, 190, 70, "health-api\nJava · REST + JDBC", "java")

    # vertical pipeline
    d.arrow("wear", "bottom", "dg", "top", label="gRPC PublishBatch")
    d.arrow("dg", "bottom", "kraw", "top")
    d.arrow("kraw", "bottom", "sp", "top")
    d.arrow("sp", "bottom", "kevt", "top")
    d.arrow("kevt", "bottom", "ad", "top")
    d.arrow("ad", "bottom", "kalr", "top")
    d.arrow("kalr", "bottom", "lg", "top")
    d.arrow("lg", "bottom", "mob", "top", label="WebSocket push")

    # writers → Postgres left edge (entering at different heights, no crossing)
    d.arrow("sp", "right", "pg", "left", color="#546e7a", label="rollups", dfrac=0.3)
    d.arrow("ad", "right", "pg", "left", color="#546e7a", label="events / alerts", dfrac=0.75)
    # readers approach from below / right
    d.arrow("ha", "top", "pg", "bottom", color="#546e7a", label="JDBC reads")
    d.arrow("iw", "left", "pg", "right", color="#546e7a", label="nightly batch")
    # read path
    d.arrow("mob", "right", "ha", "left", color="#f08c00", label="REST")

    d.write("02-high-level-architecture.excalidraw")


# ===========================================================================
# Diagram 3 — Observability plane (low-level: collector topology)
# ===========================================================================
def diagram_observability():
    d = Diagram()
    d.title_text(60, 20, "Observability plane — apps → agent / sidecar → gateway → backends")

    # tier 1: emitters
    apps = [("e-dg", 80, "device-gateway\nGo", "go"),
            ("e-sp", 270, "stream-processor\nPython", "py"),
            ("e-ha", 460, "health-api\nJava", "java"),
            ("e-lg", 650, "live-gateway\nNode.js", "node")]
    for eid, x, label, pal in apps:
        d.box(eid, x, 80, 170, 70, label, pal)
    d.box("e-ad", 900, 80, 190, 70, "anomaly-detector\nPython · sidecar pattern", "py")

    # tier 2: collection
    d.box("agent", 180, 250, 470, 90,
          "OTel Agent — DaemonSet\nk8sattributes · resource · batch\nOTLP :4317 / :4318", "agent", big=True)
    d.box("side", 900, 250, 190, 90,
          "OTel Sidecar\n127.0.0.1:4317\nkeeps 100% of alerts", "sidecar")

    # tier 3: gateway
    d.box("gw", 420, 430, 340, 100,
          "OTel Gateway — 2 replicas\nmemory_limiter · tail_sampling · batch\n+ spanmetrics connector", "gateway", big=True)

    # tier 4: backends
    d.box("prom", 300, 620, 170, 70, "Prometheus\nRED metrics :8889", "metrics")
    d.box("jaeger", 520, 620, 170, 70, "Jaeger\ndistributed traces", "traces")
    d.box("debug", 740, 620, 170, 70, "debug exporter\nlogs → stdout", "logs")

    # tier 5
    d.box("grafana", 420, 790, 220, 70, "Grafana\nunified dashboards", "grafana")

    # arrows: apps -> agent (staggered fan-in)
    for i, (eid, x, _, _) in enumerate(apps):
        d.arrow(eid, "bottom", "agent", "top", color="#2f9e44",
                dfrac=0.2 + 0.2 * i)
    d.arrow("e-ad", "bottom", "side", "top", color="#e03131", label="OTLP → 127.0.0.1")

    # collection -> gateway
    d.arrow("agent", "bottom", "gw", "top", color="#2f9e44", label="OTLP → gateway", dfrac=0.35)
    d.arrow("side", "bottom", "gw", "top", color="#e03131", label="bypasses node agent", dfrac=0.7)

    # gateway -> backends
    d.arrow("gw", "bottom", "prom", "top", color="#f08c00", label="metrics", sfrac=0.25)
    d.arrow("gw", "bottom", "jaeger", "top", color="#1971c2", label="traces", sfrac=0.5)
    d.arrow("gw", "bottom", "debug", "top", color="#90a4ae", label="logs", sfrac=0.75)

    # backends -> grafana
    d.arrow("prom", "bottom", "grafana", "top", color="#7048e8", dfrac=0.35)
    d.arrow("jaeger", "bottom", "grafana", "top", color="#7048e8", dfrac=0.7)

    d.write("03-observability-plane.excalidraw")


if __name__ == "__main__":
    diagram_sidecar_vs_daemonset()
    diagram_highlevel()
    diagram_observability()
    print("done")
