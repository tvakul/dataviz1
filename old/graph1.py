import marimo

__generated_with = "0.21.1"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import micropip

    return (micropip,)


@app.cell
async def _(micropip):
    await micropip.install("svg.py")
    return


@app.cell
def _():
    import sys

    def is_wasm():
        return (
            sys.platform == "emscripten"
            or "pyodide" in sys.modules
        )

    print(is_wasm())
    return


@app.cell
async def _(micropip):
    import sqlite3
    import pandas as pd
    import glob
    import math
    import svg
    from collections import defaultdict, Counter
    await micropip.install("sqlglot")  
    import duckdb

    return Counter, defaultdict, math, pd, svg


@app.cell
def _():
    # if is_wasm() == False:   
    #     duckdb.sql("""
    #     INSTALL sqlite;
    #     LOAD sqlite;
    #     ATTACH 'database_jour.db' (TYPE sqlite);
    #     USE database_jour;
    #     ATTACH 'database_gov.db' (TYPE sqlite);
    #     USE database_gov;
    #     """)
    # else:
    #     pass
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Sentiment map
    """)
    return


@app.cell
def _(pd):
    bias_persons = None


    # if is_wasm() == False:   
    #     # General bias
    #     # Average sentiment of all persons (for the center circle)
    #     bias_persons = duckdb.sql("""
    #     with bias_pers as (
    #         select people_id, 
    #                avg(sentiment) as avg_sentiment
    #         from (
    #                select people_id,
    #                case when contains(industry, 'vessel') and contains(industry, 'tourism') then null
    #                     when contains(industry, 'vessel') then sentiment
    #                     when contains(industry, 'tourism') then sentiment * -1
    #                     else null end as sentiment 
    #                from database_jour.discussion_people_participations
    #                union all
    #                select people_id,
    #                case when contains(industry, 'vessel') and contains(industry, 'tourism') then null  
    #                     when contains(industry, 'vessel') then sentiment
    #                     when contains(industry, 'tourism') then sentiment * -1
    #                     else null end as sentiment
    #                from database_jour.plan_people_participations
    #         )         
    #         group by people_id)
    #     select a.people_id, b.name, role, avg_sentiment 
    #     from bias_pers a,
    #          database_jour.people b
    #     where a.people_id = b.people_id

    #     """).to_df()


    #     bias_persons['honesty'] = True
    #     bias_persons.loc[bias_persons['name'] == 'Tante Titan', 'honesty'] = False
    #     bias_persons['idx'] = bias_persons['role'] .map({"Committee Chair": 1, "Vice Chair": 2, "Treasurer": 6, "Member": 3})
    #     bias_persons = bias_persons.sort_values(by='idx')
    #     bias_persons.to_json("bias_persons.json")
    # else:
    bias_persons = pd.read_json("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/bias_persons.json")

    bias_persons
    return (bias_persons,)


@app.cell
def _(pd):
    nodes = None
    # if is_wasm() == False:
    #     nodes = duckdb.sql("""
    #     with nodes_a as (
    #         select b.topic_id, 
    #                a.discussion_id as target_id, 
    #                'discussion' as target_type,
    #                a.people_id,
    #                case when contains(industry, 'vessel') and contains(industry, 'tourism') then 0
    #                     when contains(industry, 'vessel') then sentiment
    #                     when contains(industry, 'tourism') then sentiment * -1
    #                else 0 end as sentiment,
    #                sentiment as sentiment_raw,
    #                case when contains(industry, 'vessel') and contains(industry, 'tourism') then 'both'
    #                     when contains(industry, 'vessel') then 'fishing'
    #                     when contains(industry, 'tourism') then 'tourism'
    #                else 'other' end as industry
    #         from database_jour.discussion_people_participations a
    #         left join database_jour.discussion_topics  b 
    #         on a.discussion_id = b.discussion_id
    #         where  contains(industry, 'vessel') or contains(industry, 'tourism') 
    #         union all
    #         select b.topic_id, 
    #                a.plan_id as target_id, 
    #                'plan' as target_type,
    #                a.people_id,
    #                case when contains(industry, 'vessel') and contains(industry, 'tourism') then 0
    #                     when contains(industry, 'vessel') then sentiment
    #                     when contains(industry, 'tourism') then sentiment * -1           
    #                else 0 end as sentiment,
    #                sentiment as sentiment_raw,
    #                case when contains(industry, 'vessel') and contains(industry, 'tourism') then 'both'
    #                     when contains(industry, 'vessel') then 'fishing'
    #                     when contains(industry, 'tourism') then 'tourism'
    #                else 'other' end as industry
    #         from database_jour.plan_people_participations a
    #         left join database_jour.plan_topics  b 
    #         on a.plan_id = b.plan_id
    #         where  contains(industry, 'vessel') or contains(industry, 'tourism') 
    #     ),
    #     in_gov_data_chk as (
    #         select a.discussion_id as target_id, a.people_id, FALSE as in_gov_data
    #         from database_jour.discussion_people_participations a    
    #         left join database_gov.discussion_people_participations b
    #         on a.discussion_id = b.discussion_id 
    #         where b.discussion_id is null
    #         union all 
    #         select a.plan_id as target_id, a.people_id, FALSE as in_gov_data
    #         from database_jour.plan_people_participations a    
    #         left join database_gov.plan_people_participations b
    #         on a.plan_id = b.plan_id 
    #         where b.plan_id is null
    #     )
    #     select a.*, COALESCE(b.in_gov_data, TRUE) as in_gov_data
    #     from nodes_a a
    #     left join in_gov_data_chk b
    #     on a.target_id= b.target_id and a.people_id = b.people_id
    #     """).to_df()
    #     nodes.loc[(pd.isna(nodes['topic_id'])) &(nodes['target_id'].str.startswith('waterfront_market')), 'topic_id'] = 'waterfront_market'
    #     nodes.to_json("nodes.json")
    # else:
    nodes = pd.read_json("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/nodes.json")

    nodes
    return (nodes,)


@app.cell
def _(nodes):
    nodes.drop_duplicates(subset=['industry', 'topic_id']).groupby('industry').agg('count')
    return


@app.cell
def _(Counter, bias_persons, defaultdict, math, mo, nodes, svg):
    persons  = bias_persons.to_dict(orient="records")

    # ── svg.py subclasses ────────────────────────────────────────────────────
    class DataRect(svg.Rect):
        def __init__(self, data_fill, **kwargs):
            self._data_fill = data_fill
            super().__init__(**kwargs)          # data_fill not passed up
        def as_str(self):
            s = super().as_str()
            # svg.py emits -data-fill="" too if it got it — strip that first
            import re
            s = re.sub(r'\s-data-fill="[^"]*"', '', s)
            return s.replace("/>", f' data-fill="{self._data_fill}"/>', 1)

    class DataPolygon(svg.Polygon):
        def __init__(self, data_fill, **kwargs):
            self._data_fill = data_fill
            super().__init__(**kwargs)
        def as_str(self):
            import re
            s = super().as_str()
            s = re.sub(r'\s-data-fill="[^"]*"', '', s)
            return s.replace("/>", f' data-fill="{self._data_fill}"/>', 1)

    class DataG(svg.G):
        def __init__(self, data_targets="", data_topics="", data_pid="", **kwargs):
            self._data_targets = data_targets
            self._data_topics  = data_topics
            self._data_pid     = data_pid
            super().__init__(**kwargs)
        def as_str(self):
            import re
            s = super().as_str()
            # Strip any -data-* attrs svg.py injected by default
            s = re.sub(r'\s-data-[a-z_]+="[^"]*"', '', s)
            # Explicitly inject the correct ones
            attrs = f' data-targets="{self._data_targets}" data-topics="{self._data_topics}" data-pid="{self._data_pid}"'
            return s.replace("<g ", f"<g{attrs} ", 1)
    # ── helpers ──────────────────────────────────────────────────────────────
    def interp(t, c0, c1):
        r, g, b = (int(c0[i] + (c1[i] - c0[i]) * t) for i in range(3))
        return f"rgb({r},{g},{b})"

    def color_for(s):
        t = min(abs(s), 1.0)
        return interp(t, (255, 255, 255),
                      (46, 204, 113) if s >= 0 else (231, 76, 60))

    def text_ink(s):
        return "#111" if abs(s) < 0.55 else "white"

    def safe_id(s):
        return str(s).replace(" ", "_").replace(".", "_").replace("-", "_")

    # ── glyphs ───────────────────────────────────────────────────────────────
    SCALE = 1.8

    def person_glyph(px, py, fill, s=SCALE):
        return svg.G(elements=[
            svg.Circle(cx=px, cy=py-5*s, r=3*s,
                       fill=fill, stroke="#333", stroke_width=0.8),
            svg.Path(d=(f"M {px-5*s} {py+7*s} L {px-5*s} {py} "
                        f"A {5*s} {4*s} 0 0 1 {px+5*s} {py} "
                        f"L {px+5*s} {py+7*s} Z"),
                     fill=fill, stroke="#333", stroke_width=0.8),
        ])

    def king_crown(px, py):
        c = py - 15
        return svg.G(elements=[
            svg.Path(d=(f"M {px-8} {c+3} L {px-8} {c-1} L {px-5} {c-5} "
                        f"L {px-2.5} {c-1} L {px} {c-7} L {px+2.5} {c-1} "
                        f"L {px+5} {c-5} L {px+8} {c-1} L {px+8} {c+3} Z"),
                     fill="#f1c40f", stroke="#8b6914", stroke_width=0.7),
            svg.Circle(cx=px-5, cy=c-5.5, r=1,   fill="#e74c3c"),
            svg.Circle(cx=px,   cy=c-7.5, r=1.3, fill="#3498db"),
            svg.Circle(cx=px+5, cy=c-5.5, r=1,   fill="#e74c3c"),
        ])

    def queen_crown(px, py):
        c = py - 15
        return svg.G(elements=[
            svg.Path(d=(f"M {px-7} {c+3} L {px-7} {c} L {px-4.5} {c-4} "
                        f"L {px-2.5} {c} L {px-1} {c-6} L {px+1} {c-6} "
                        f"L {px+2.5} {c} L {px+4.5} {c-4} "
                        f"L {px+7} {c} L {px+7} {c+3} Z"),
                     fill="#ecf0f1", stroke="#7f8c8d", stroke_width=0.7),
            svg.Circle(cx=px, cy=c-6.5, r=1.3, fill="#9b59b6"),
        ])

    def chest(px, py):
        bx, by = px + 13, py + 4
        return svg.G(elements=[
            svg.Ellipse(cx=bx, cy=by, rx=6, ry=2,
                        fill="#a0522d", stroke="#333", stroke_width=0.5),
            svg.Rect(x=bx-6, y=by, width=12, height=7,
                     fill="#8b4513", stroke="#333", stroke_width=0.5),
            svg.Rect(x=bx-1, y=by+1, width=2, height=2.5,
                     fill="#f1c40f", stroke="#333", stroke_width=0.3),
            svg.Circle(cx=bx-2,   cy=by-2,   r=1.2,
                       fill="#f1c40f", stroke="#333", stroke_width=0.3),
            svg.Circle(cx=bx+1.5, cy=by-1.5, r=1,
                       fill="#f1c40f", stroke="#333", stroke_width=0.3),
        ])

    def halo(px, py, has_crown):
        return svg.Ellipse(cx=px, cy=py-(24 if has_crown else 17),
                           rx=8, ry=2.5, fill="none",
                           stroke="#f1c40f", stroke_width=1.8)

    def devil_horns(px, py, s=SCALE):
        head_cy  = py - 5 * s
        head_top = head_cy - 3 * s
        return svg.G(elements=[
            svg.Path(d=f"M {px-3.5} {head_top} L {px-2} {head_top-6} L {px-0.5} {head_top} Z",
                     fill="#c0392b", stroke="#8b2820", stroke_width=0.4),
            svg.Path(d=f"M {px+0.5} {head_top} L {px+2} {head_top-6} L {px+3.5} {head_top} Z",
                     fill="#c0392b", stroke="#8b2820", stroke_width=0.4),
        ])

    # ── data aggregation ─────────────────────────────────────────────────────
    agg = (nodes
           .groupby(["topic_id", "target_id", "target_type", "industry"])
           .agg(avg_sentiment=("sentiment", "mean"),
                avg_sentiment_raw=("sentiment_raw", "mean"), # NEW: Capture raw
                people=("people_id", lambda x: frozenset(x)))
           .reset_index())

    topics = {}
    for _, row in agg.iterrows():
        tid = row["topic_id"]
        if tid not in topics:
            topics[tid] = {"targets": [], "sentiments": [], "sentiments_raw": [], "industries": []}
        topics[tid]["targets"].append(row.to_dict())
        topics[tid]["sentiments"].append(row["avg_sentiment"])
        topics[tid]["sentiments_raw"].append(row["avg_sentiment_raw"])
        topics[tid]["industries"].append(row["industry"])

    for tid, t in topics.items():
        t["avg_sentiment"] = sum(t["sentiments"]) / len(t["sentiments"])
        t["avg_sentiment_raw"] = sum(t["sentiments_raw"]) / len(t["sentiments_raw"])
        t["industry"] = Counter(t["industries"]).most_common(1)[0][0]

    fishing_topics = [(tid, t) for tid, t in topics.items() if t["industry"] == "fishing"]
    tourism_topics = [(tid, t) for tid, t in topics.items() if t["industry"] == "tourism"]
    both_topics    = [(tid, t) for tid, t in topics.items() if t["industry"] in ("both", "other")]

    # ── canvas ───────────────────────────────────────────────────────────────
    CELL     = 210
    MARGIN   = 70
    R_CENTER = 42
    R_RING   = 130
    R_CLOUD  = 52

    W        = max(900, max(len(fishing_topics), len(tourism_topics), 1) * CELL + 2 * MARGIN)
    ZONE_H   = CELL + 80
    CENTRE_H = R_RING * 2 + 160
    H        = ZONE_H + CENTRE_H + ZONE_H + 2 * MARGIN

    FISH_Y = MARGIN
    TOUR_Y = MARGIN + ZONE_H + CENTRE_H
    cx     = W / 2
    cy     = MARGIN + ZONE_H + CENTRE_H / 2

    # ── topic placement ──────────────────────────────────────────────────────
    def place_in_band(topic_list, band_y, band_h):
        n = len(topic_list)
        if n == 0:
            return {}
        out = {}
        for i, (tid, _) in enumerate(topic_list):
            x = MARGIN + CELL/2 + i * ((W - 2*MARGIN - CELL) / max(n-1, 1))
            y = band_y + band_h / 2
            out[tid] = (x, y)
        return out

    cloud_pos = {}
    cloud_pos.update(place_in_band(fishing_topics, FISH_Y, ZONE_H))
    cloud_pos.update(place_in_band(tourism_topics, TOUR_Y, ZONE_H))
    for i, (tid, _) in enumerate(both_topics):
        side   =  1 if i % 2 == 0 else -1
        offset = (i // 2 + 1) * 150
        cloud_pos[tid] = (cx + side * (R_RING + offset), cy)

    # ── target positions ─────────────────────────────────────────────────────
    target_pos = {}
    for tid, (gx, gy) in cloud_pos.items():
        tlist = topics[tid]["targets"]
        n     = len(tlist)
        r     = R_CLOUD if n > 1 else 0
        for j, t in enumerate(tlist):
            ang = -math.pi/2 + 2*math.pi*j / max(n, 1)
            target_pos[t["target_id"]] = (
                gx + r * math.cos(ang),
                gy + r * math.sin(ang),
                t,
            )

    # ── person positions ─────────────────────────────────────────────────────
    person_pos = {}
    for i, p in enumerate(persons):
        pid = p.get("people_id", p.get("id", i))
        ang = -math.pi/2 + 2*math.pi*i / len(persons)
        person_pos[pid] = (
            cx + R_RING * math.cos(ang),
            cy + R_RING * math.sin(ang),
            p,
        )

    # ── person → topic & target sentiments ───────────────────────────────────
    person_target_sent = nodes.groupby(['people_id', 'target_id'])['sentiment'].mean().to_dict()
    person_target_raw  = nodes.groupby(['people_id', 'target_id'])['sentiment_raw'].mean().to_dict()

    # NEW: Track if the (person, target) combo exists in gov data.
    # min() safely evaluates boolean False (0) < True (1).
    person_target_gov  = nodes.groupby(['people_id', 'target_id'])['in_gov_data'].min().to_dict()

    person_topic_sent  = nodes.groupby(['people_id', 'topic_id'])['sentiment'].mean().to_dict()
    person_topic_raw   = nodes.groupby(['people_id', 'topic_id'])['sentiment_raw'].mean().to_dict()

    pid_to_targets = defaultdict(dict)
    pid_to_topics  = defaultdict(dict)

    for (pid, tgt), sent in person_target_sent.items():
        if pid in person_pos:
            raw_s = person_target_raw.get((pid, tgt), sent)
            is_gov = person_target_gov.get((pid, tgt), True)

            # CHANGED: Now storing a 3-tuple with the gov data flag
            pid_to_targets[pid][tgt] = (sent, raw_s, is_gov)

    for (pid, top), sent in person_topic_sent.items():
        if pid in person_pos:
            raw_s = person_topic_raw.get((pid, top), sent)
            pid_to_topics[pid][top] = (sent, raw_s)

    # ── z-order buckets ──────────────────────────────────────────────────────
    bg, zone_labels, clouds, edges, target_nodes, ring_layer, centre_layer, legend_layer = \
        [], [], [], [], [], [], [], []

    mean_val = bias_persons["avg_sentiment"].mean()
    pale     = interp(min(abs(mean_val), 1.0) * 0.3, (255, 255, 255),
                      (46, 204, 113) if mean_val >= 0 else (231, 76, 60))

    bg.append(svg.Defs(elements=[
        # Center mean gradient
        svg.RadialGradient(id="g_mean", cx="50%", cy="50%", r="50%", elements=[
            svg.Stop(offset="0%",   stop_color=pale),
            svg.Stop(offset="100%", stop_color=color_for(mean_val)),
        ]),
        # NEW: Legend color gradient
        svg.LinearGradient(id="grad_legend", x1="0%", y1="0%", x2="100%", y2="0%", elements=[
            svg.Stop(offset="0%", stop_color="#e74c3c"),   # Negative (Red)
            svg.Stop(offset="50%", stop_color="#ffffff"),  # Neutral (White)
            svg.Stop(offset="100%", stop_color="#2ecc71"), # Positive (Green)
        ]),
    ]))

    bg += [
        svg.Rect(x=0, y=FISH_Y, width=W, height=ZONE_H,
                 fill="#3498db", opacity=0.05,
                 stroke="#2980b9", stroke_width=1, stroke_dasharray="6,4"),
        svg.Rect(x=0, y=TOUR_Y, width=W, height=ZONE_H,
                 fill="#f1c40f", opacity=0.05,
                 stroke="#f39c12", stroke_width=1, stroke_dasharray="6,4"),
    ]

    # --- NEW: Build the Legend ---
    leg_x, leg_y = 25, cy - 50 

    legend_layer += [
        # Color Scale
        svg.Text(x=leg_x, y=leg_y, text="Sentiment for fishing", font_size=12, font_weight="bold", fill="#555"),
        svg.Rect(x=leg_x, y=leg_y + 8, width=100, height=10, fill="url(#grad_legend)", stroke="#999", stroke_width=0.5),
        svg.Text(x=leg_x, y=leg_y + 32, text="-1", font_size=10, fill="#555", text_anchor="middle"),
        svg.Text(x=leg_x+50, y=leg_y + 32, text="0", font_size=10, fill="#555", text_anchor="middle"),
        svg.Text(x=leg_x+100, y=leg_y + 32, text="+1", font_size=10, fill="#555", text_anchor="middle"),

        # Shape Key
        svg.Text(x=leg_x, y=leg_y + 58, text="Item Type", font_size=12, font_weight="bold", fill="#555"),

        # Pentagon Guide
        svg.Polygon(
            points=f"{leg_x+6},{leg_y+72-7} {leg_x+6+7*0.951},{leg_y+72-7*0.309} {leg_x+6+7*0.588},{leg_y+72+7*0.809} {leg_x+6-7*0.588},{leg_y+72+7*0.809} {leg_x+6-7*0.951},{leg_y+72-7*0.309}", 
            fill="#ccc", stroke="#999"
        ),
        svg.Text(x=leg_x+18, y=leg_y + 76, text="Discussion", font_size=11, fill="#555"),

        # Square Guide
        svg.Rect(x=leg_x, y=leg_y + 88, width=12, height=12, fill="#ccc", stroke="#999"),
        svg.Text(x=leg_x+18, y=leg_y + 98, text="Plan", font_size=11, fill="#555"),

        # --- NEW: Gov Data Status Key ---
        svg.Text(x=leg_x, y=leg_y + 126, text="Gov Data", font_size=12, font_weight="bold", fill="#555"),

        # Halo (Complete)
        person_glyph(leg_x + 6, leg_y + 150, fill="#ccc"),
        halo(leg_x + 6, leg_y + 150, has_crown=False),
        svg.Text(x=leg_x+18, y=leg_y + 146, text="Complete", font_size=11, fill="#555"),

        # Horns (Missing Info)
        person_glyph(leg_x + 6, leg_y + 180, fill="#ccc"),
        devil_horns(leg_x + 6, leg_y + 180),
        svg.Text(x=leg_x+18, y=leg_y + 176, text="Missing Info", font_size=11, fill="#555"),
    ]


    left_edge = 10 

    zone_labels = [
        # Top Left: Fishing
        svg.Text(x=left_edge, y=FISH_Y + 25, text="Pro-fishing topics",
                 text_anchor="start", font_size=14,
                 fill="#2980b9", font_weight="bold"),

        # Top Left: Tourism
        svg.Text(x=left_edge, y=TOUR_Y + 25, text="Pro-tourism topics",
                 text_anchor="start", font_size=14,
                 fill="#d68910", font_weight="bold"),
    ]

    # Top Left: Middle Part (Both / Other)
    if both_topics:
        zone_labels.append(svg.Text(
            x=left_edge, y=cy - R_RING - 15, text="Both",
            text_anchor="start", font_size=12,
            fill="#888", font_style="italic"
        ))

    for tid, (gx, gy) in cloud_pos.items():
        t         = topics[tid]
        n         = len(t["targets"])
        blob_r    = R_CLOUD + 8 + n * 3
        stid      = safe_id(tid)
        avg_s     = t["avg_sentiment"]
        avg_s_raw = t["avg_sentiment_raw"] # Extract raw sentiment

        clouds += [
            svg.Circle(cx=gx, cy=gy, r=blob_r,
                       fill=color_for(avg_s), opacity=0.18,
                       stroke=color_for(avg_s), stroke_width=1.2, stroke_dasharray="4,3"),

            svg.Text(x=gx, y=gy - blob_r - 8, text=str(tid),
                     text_anchor="middle", font_size=12, fill="#444", font_weight="bold"),

            # CHANGED: Display avg_s_raw instead of avg_s
            svg.Text(x=gx, y=gy, text=f"{avg_s_raw:+.2f}",
                     id=f"group_sent_{stid}", class_="group-sent",
                     text_anchor="middle", dominant_baseline="central", font_size=14,
                     fill=color_for(avg_s), font_weight="bold")
        ]

    for pid, topics_dict in pid_to_topics.items():
        px, py, p = person_pos[pid]
        spid = safe_id(pid)
        for top_id, (p_sent, p_raw) in topics_dict.items(): # CHANGED: unpack tuple
            if top_id not in cloud_pos: 
                continue
            gx, gy = cloud_pos[top_id]
            stop = safe_id(top_id)
            stroke_w = max(0.8, abs(p_sent) * 10)
            col = color_for(p_sent)

            t = topics[top_id]
            n = len(t["targets"])
            blob_r = R_CLOUD + 8 + n * 3

            dx, dy = px - gx, py - gy
            dist = math.hypot(dx, dy)
            if dist > 0:
                end_x, end_y = gx + (dx / dist) * blob_r, gy + (dy / dist) * blob_r
            else:
                end_x, end_y = gx, gy

            edges.append(svg.Line(
                x1=px, y1=py, x2=end_x, y2=end_y,
                stroke=col, stroke_width=stroke_w, opacity=0.75,
                id=f"edge_{spid}_{stop}", style="display:none",
            ))

    for tid_key, (tx, ty, data) in target_pos.items():
        fill = color_for(data["avg_sentiment"])
        stid = safe_id(tid_key)
        base = dict(id=f"node_{stid}", fill=fill, opacity="1", stroke="#999", stroke_width=1.2)

        if data["target_type"] == "discussion":
            sz = 14
            c1, s1 = 0.951, 0.309 
            c2, s2 = 0.588, 0.809 
            pts = f"{tx},{ty-sz} {tx+sz*c1},{ty-sz*s1} {tx+sz*c2},{ty+sz*s2} {tx-sz*c2},{ty+sz*s2} {tx-sz*c1},{ty-sz*s1}"
            target_nodes.append(DataPolygon(data_fill=fill, points=pts, **base))
        else:
            sz = 11
            target_nodes.append(DataRect(data_fill=fill, x=tx-sz, y=ty-sz, width=sz*2, height=sz*2, **base))

        # CHANGED: Format raw sentiment string and add specific element IDs
        val_str = f"{data['avg_sentiment_raw']:+.1f}".replace("+0.0", "0.0")

        target_nodes.append(svg.Text(
            x=tx, y=ty, text=val_str, id=f"target_text_bg_{stid}", class_="target-sent",
            font_size=9, fill="none", stroke="rgba(255, 255, 255, 0.8)", stroke_width=2, stroke_linejoin="round",
            text_anchor="middle", dominant_baseline="central", style="pointer-events:none;" 
        ))
        target_nodes.append(svg.Text(
            x=tx, y=ty, text=val_str, id=f"target_text_fg_{stid}", class_="target-sent",
            font_size=9, fill="#111", font_weight="bold",
            text_anchor="middle", dominant_baseline="central", style="pointer-events:none;"
        ))

    for pid, (px, py, p) in person_pos.items():
        role      = p.get("role", "")
        honest    = p.get("honesty", True)
        has_crown = role in ("Committee Chair", "Vice Chair")
        spid      = safe_id(pid)

        # Store explicit target colors mapped to this person
        connected_targets = [f"{safe_id(t)}:{color_for(s_biased)}:{s_raw:+.1f}:{1 if is_gov else 0}" for t, (s_biased, s_raw, is_gov) in pid_to_targets.get(pid, {}).items()]
        connected_topics  = [f"{safe_id(t)}:{color_for(s_biased)}:{s_raw:+.2f}" for t, (s_biased, s_raw) in pid_to_topics.get(pid, {}).items()]

        p_sent = p.get("avg_sentiment", 0)
        g_els = [person_glyph(px, py, color_for(p_sent))]
        if role == "Treasurer":       g_els.append(chest(px, py))
        if not honest:                g_els.append(devil_horns(px, py))
        if role == "Committee Chair": g_els.append(king_crown(px, py))
        elif role == "Vice Chair":    g_els.append(queen_crown(px, py))
        if honest:                    g_els.append(halo(px, py, has_crown))

        name = p.get("name", "Unknown")
        g_els.append(svg.Text(
            x=px+12, y=py+4, text=f"{name} ({p_sent:+.2f})",
            font_size=11, fill="#333", id=f"text_{spid}", class_="person-label"
        ))

        ring_layer.append(DataG(
            id=f"person_{spid}",
            elements=g_els,
            style="cursor:pointer",
            data_targets=";".join(connected_targets),
            data_topics=";".join(connected_topics),
            data_pid=spid,
        ))

        # connected_targets = [f"{safe_id(t)}:{color_for(s_biased)}:{s_raw:+.1f}" for t, (s_biased, s_raw) in pid_to_targets.get(pid, {}).items()]
        # connected_topics  = [f"{safe_id(t)}:{color_for(s_biased)}:{s_raw:+.2f}" for t, (s_biased, s_raw) in pid_to_topics.get(pid, {}).items()]

        p_sent = p.get("avg_sentiment", 0)

    centre_layer += [
        svg.Circle(cx=cx, cy=cy, r=R_RING,
                   fill="none", stroke="#ddd", stroke_dasharray="3,3"),
        svg.Circle(cx=cx, cy=cy, r=R_CENTER,
                   fill="url(#g_mean)", stroke="#333", stroke_width=2),
        svg.Text(x=cx, y=cy, text=f"{mean_val:+.2f}",
                 fill=text_ink(mean_val), text_anchor="middle",
                 dominant_baseline="central", font_size=14, font_weight="bold"),
    ]

    all_el  = bg + legend_layer + zone_labels + clouds + edges + target_nodes + ring_layer + centre_layer
    svg_str = svg.SVG(height=H, width=W, elements=all_el).as_str()

    _JS = """
    <style>
    body {
       font-family: sans-serif;
       /* NEW: Center contents horizontally and remove default margins */
       display: flex;
       justify-content: center;
       margin: 0;
       padding-top: 20px; /* Optional: adds a little breathing room at the top */
    }
    .glow-target {
       filter: drop-shadow(0 0 5px #e74c3c) drop-shadow(0 0 5px #e74c3c);
       stroke: #c0392b !important;
       stroke-width: 2.5px !important;
    }
    </style>
    <script>
    /* Minification-safe: No single-line comments used */
    (function initGraph() {
      const persons = document.querySelectorAll('[id^="person_"]');
      if (persons.length === 0) {
          setTimeout(initGraph, 50);
          return;
      }

      document.querySelectorAll('.group-sent').forEach(el => {
          el.setAttribute('data-default', el.textContent);
          el.setAttribute('data-default-fill', el.getAttribute('fill'));
      });

      document.querySelectorAll('.target-sent').forEach(el => {
          el.setAttribute('data-default', el.textContent);
      });

      function resetAll() {
        document.querySelectorAll('[id^="edge_"]').forEach(e => e.style.display = 'none');
        document.querySelectorAll('[id^="node_"]').forEach(n => {
          n.setAttribute('fill', n.getAttribute('data-fill'));
          n.setAttribute('opacity', '1');
          n.setAttribute('stroke', '#999');
          n.classList.remove('glow-target'); 
        });
        document.querySelectorAll('.person-label').forEach(l => l.style.display = '');

        document.querySelectorAll('.group-sent').forEach(el => {
          el.textContent = el.getAttribute('data-default');
          el.setAttribute('fill', el.getAttribute('data-default-fill'));
        });

        document.querySelectorAll('.target-sent').forEach(el => {
          el.textContent = el.getAttribute('data-default');
          el.style.display = ''; 
        });
      }

      persons.forEach(g => {
        const pid = g.getAttribute('data-pid');
        const targetStrs = (g.getAttribute('data-targets') || '').split(';').filter(Boolean);
        const topicStrs  = (g.getAttribute('data-topics') || '').split(';').filter(Boolean);

        const targetMap = {};
        targetStrs.forEach(pair => {
            const parts = pair.split(':');
            if (parts.length >= 4) {
                targetMap[parts[0]] = { color: parts[1], text: parts[2], is_gov: parts[3] === '1' };
            }
        });

        const topicMap = {};
        topicStrs.forEach(pair => {
            const parts = pair.split(':');
            if (parts.length >= 3) topicMap[parts[0]] = { color: parts[1], text: parts[2] };
        });

        g.addEventListener('mouseenter', () => {
          document.querySelectorAll('.person-label').forEach(l => {
              if (l.id !== 'text_' + pid) l.style.display = 'none';
          });

          document.querySelectorAll('[id^="edge_"]').forEach(e => e.style.display = 'none');
          Object.keys(topicMap).forEach(stop => {
            const edge = document.getElementById('edge_' + pid + '_' + stop);
            if (edge) edge.style.display = '';
          });

          document.querySelectorAll('.group-sent').forEach(el => {
            const stid = el.id.replace('group_sent_', '');
            if (topicMap[stid]) {
                el.textContent = topicMap[stid].text;
                el.setAttribute('fill', topicMap[stid].color);
            } else {
                el.textContent = 'N/A';
                el.setAttribute('fill', '#bbb');
            }
          });

          document.querySelectorAll('[id^="node_"]').forEach(n => {
            const stid = n.id.replace('node_', '');
            if (targetMap[stid]) {
              n.setAttribute('fill', targetMap[stid].color);
              n.setAttribute('opacity', '1');
              n.setAttribute('stroke', '#333');

              if (!targetMap[stid].is_gov) {
                  n.classList.add('glow-target');
              }
            } else {
              n.setAttribute('fill', '#ccc');
              n.setAttribute('opacity', '0.35');
              n.setAttribute('stroke', '#999');
            }
          });

          document.querySelectorAll('.target-sent').forEach(el => {
            const stid = el.id.replace('target_text_bg_', '').replace('target_text_fg_', '');
            if (targetMap[stid]) {
                el.textContent = targetMap[stid].text;
                el.style.display = '';
            } else {
                el.style.display = 'none'; 
            }
          });
        });

        g.addEventListener('mouseleave', resetAll);
      });

      resetAll();
    })();
    </script>
    """

    mo.vstack([mo.iframe(svg_str + _JS, height="1000px", width="1200px")], align="center")
    return


if __name__ == "__main__":
    app.run()
