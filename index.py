import marimo

__generated_with = "0.21.1"
app = marimo.App(width="full", app_title="Data Visualization")


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


@app.cell
def _():
    ## **Sentiment map**
    return


@app.cell
def _(mo, pd):
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

    try:
        bias_persons = pd.read_json(str(mo.notebook_location() / "data" / "bias_persons.json"))
    except:
        bias_persons = pd.read_json("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/bias_persons.json") 

    # bias_persons
    return (bias_persons,)


@app.cell
def _(mo, pd):
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
    try:
        nodes = pd.read_json(str(mo.notebook_location() / "data" / "nodes.json"))
    except:
        nodes = pd.read_json("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/data/nodes.json")


    # nodes
    return (nodes,)


@app.cell
def _():
    # nodes.drop_duplicates(subset=['industry', 'topic_id']).groupby('industry').agg('count')
    return


@app.cell
def _(Counter, bias_persons, defaultdict, math, mo, nodes, svg):
    persons  = bias_persons.to_dict(orient="records")

    # ── svg.py subclasses ────────────────────────────────────────────────────
    class DataRect(svg.Rect):
        def __init__(self, data_fill, **kwargs):
            self._data_fill = data_fill
            super().__init__(**kwargs)          
        def as_str(self):
            s = super().as_str()
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
            s = re.sub(r'\s-data-[a-z_]+="[^"]*"', '', s)
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
    SCALE = 1.3 # REDUCED from 1.8

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
        c = py - 12 # Adjusted for scale
        return svg.G(elements=[
            svg.Path(d=(f"M {px-7} {c+3} L {px-7} {c-1} L {px-4} {c-4} "
                        f"L {px-2} {c-1} L {px} {c-6} L {px+2} {c-1} "
                        f"L {px+4} {c-4} L {px+7} {c-1} L {px+7} {c+3} Z"),
                     fill="#f1c40f", stroke="#8b6914", stroke_width=0.7),
            svg.Circle(cx=px-4, cy=c-4.5, r=0.8,   fill="#e74c3c"),
            svg.Circle(cx=px,   cy=c-6.5, r=1.0, fill="#3498db"),
            svg.Circle(cx=px+4, cy=c-4.5, r=0.8,   fill="#e74c3c"),
        ])

    def queen_crown(px, py):
        c = py - 12 # Adjusted for scale
        return svg.G(elements=[
            svg.Path(d=(f"M {px-6} {c+3} L {px-6} {c} L {px-4} {c-3} "
                        f"L {px-2} {c} L {px-1} {c-5} L {px+1} {c-5} "
                        f"L {px+2} {c} L {px+4} {c-3} "
                        f"L {px+6} {c} L {px+6} {c+3} Z"),
                     fill="#ecf0f1", stroke="#7f8c8d", stroke_width=0.7),
            svg.Circle(cx=px, cy=c-5.5, r=1.0, fill="#9b59b6"),
        ])

    def chest(px, py):
        bx, by = px + 10, py + 3
        return svg.G(elements=[
            svg.Ellipse(cx=bx, cy=by, rx=5, ry=1.8,
                        fill="#a0522d", stroke="#333", stroke_width=0.5),
            svg.Rect(x=bx-5, y=by, width=10, height=6,
                     fill="#8b4513", stroke="#333", stroke_width=0.5),
            svg.Rect(x=bx-1, y=by+1, width=2, height=2,
                     fill="#f1c40f", stroke="#333", stroke_width=0.3),
            svg.Circle(cx=bx-1.5, cy=by-1.5, r=1.0,
                       fill="#f1c40f", stroke="#333", stroke_width=0.3),
            svg.Circle(cx=bx+1.5, cy=by-1.5, r=0.8,
                       fill="#f1c40f", stroke="#333", stroke_width=0.3),
        ])

    def halo(px, py, has_crown):
        return svg.Ellipse(cx=px, cy=py-(18 if has_crown else 12),
                           rx=7, ry=2.0, fill="none",
                           stroke="#f1c40f", stroke_width=1.5)

    def devil_horns(px, py, s=SCALE):
        head_cy  = py - 5 * s
        head_top = head_cy - 3 * s
        return svg.G(elements=[
            svg.Path(d=f"M {px-3} {head_top} L {px-1.5} {head_top-5} L {px-0.5} {head_top} Z",
                     fill="#c0392b", stroke="#8b2820", stroke_width=0.4),
            svg.Path(d=f"M {px+0.5} {head_top} L {px+1.5} {head_top-5} L {px+3} {head_top} Z",
                     fill="#c0392b", stroke="#8b2820", stroke_width=0.4),
        ])

    # ── data aggregation ─────────────────────────────────────────────────────
    agg = (nodes
           .groupby(["topic_id", "target_id", "target_type", "industry"])
           .agg(avg_sentiment=("sentiment", "mean"),
                avg_sentiment_raw=("sentiment_raw", "mean"), 
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

    # ── canvas  ──────────────────────────────────────────
    CELL     = 140 
    MARGIN   = 0  
    R_CENTER = 30  
    R_RING   = 90  
    R_CLOUD  = 35  

    W        = max(900, max(len(fishing_topics), len(tourism_topics), 1) * CELL + 2 * MARGIN)
    ZONE_H   = CELL + 50
    CENTRE_H = R_RING * 2 + 100
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
        offset = (i // 2 + 1) * 110 # REDUCED from 150
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
    person_target_gov  = nodes.groupby(['people_id', 'target_id'])['in_gov_data'].min().to_dict()
    person_topic_sent  = nodes.groupby(['people_id', 'topic_id'])['sentiment'].mean().to_dict()
    person_topic_raw   = nodes.groupby(['people_id', 'topic_id'])['sentiment_raw'].mean().to_dict()

    pid_to_targets = defaultdict(dict)
    pid_to_topics  = defaultdict(dict)

    for (pid, tgt), sent in person_target_sent.items():
        if pid in person_pos:
            raw_s = person_target_raw.get((pid, tgt), sent)
            is_gov = person_target_gov.get((pid, tgt), True)
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
        svg.RadialGradient(id="g_mean", cx="50%", cy="50%", r="50%", elements=[
            svg.Stop(offset="0%",   stop_color=pale),
            svg.Stop(offset="100%", stop_color=color_for(mean_val)),
        ]),
        svg.LinearGradient(id="grad_legend", x1="0%", y1="0%", x2="100%", y2="0%", elements=[
            svg.Stop(offset="0%", stop_color="#e74c3c"),   
            svg.Stop(offset="50%", stop_color="#ffffff"),  
            svg.Stop(offset="100%", stop_color="#2ecc71"), 
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

    # --- Legend ---
    leg_x, leg_y = 15, cy - 60 

    legend_layer += [
        svg.Text(x=leg_x, y=leg_y, text="Sentiment for fishing", font_size=10, font_weight="bold", fill="#555"),
        svg.Rect(x=leg_x, y=leg_y + 8, width=90, height=8, fill="url(#grad_legend)", stroke="#999", stroke_width=0.5),
        svg.Text(x=leg_x, y=leg_y + 28, text="-1", font_size=9, fill="#555", text_anchor="middle"),
        svg.Text(x=leg_x+45, y=leg_y + 28, text="0", font_size=9, fill="#555", text_anchor="middle"),
        svg.Text(x=leg_x+90, y=leg_y + 28, text="+1", font_size=9, fill="#555", text_anchor="middle"),

        svg.Text(x=leg_x, y=leg_y + 50, text="Item Type", font_size=10, font_weight="bold", fill="#555"),
        svg.Polygon(
            points=f"{leg_x+5},{leg_y+62-6} {leg_x+5+6*0.951},{leg_y+62-6*0.309} {leg_x+5+6*0.588},{leg_y+62+6*0.809} {leg_x+5-6*0.588},{leg_y+62+6*0.809} {leg_x+5-6*0.951},{leg_y+62-6*0.309}", 
            fill="#ccc", stroke="#999"
        ),
        svg.Text(x=leg_x+15, y=leg_y + 65, text="Discussion", font_size=10, fill="#555"),
        svg.Rect(x=leg_x-1, y=leg_y + 75, width=10, height=10, fill="#ccc", stroke="#999"),
        svg.Text(x=leg_x+15, y=leg_y + 83, text="Plan", font_size=10, fill="#555"),

        svg.Text(x=leg_x, y=leg_y + 110, text="Gov Data", font_size=10, font_weight="bold", fill="#555"),
        person_glyph(leg_x + 5, leg_y + 130, fill="#ccc"),
        halo(leg_x + 5, leg_y + 130, has_crown=False),
        svg.Text(x=leg_x+15, y=leg_y + 127, text="Complete", font_size=10, fill="#555"),
        person_glyph(leg_x + 5, leg_y + 155, fill="#ccc"),
        devil_horns(leg_x + 5, leg_y + 155),
        svg.Text(x=leg_x+15, y=leg_y + 152, text="Missing Info", font_size=10, fill="#555"),
    ]

    left_edge = 10 
    zone_labels = [
        svg.Text(x=left_edge, y=FISH_Y + 10, text="Pro-fishing topics",
                 text_anchor="start", font_size=12, fill="#2980b9", font_weight="bold"),
        svg.Text(x=left_edge, y=TOUR_Y + 10, text="Pro-tourism topics",
                 text_anchor="start", font_size=12, fill="#d68910", font_weight="bold"),
    ]

    if both_topics:
        zone_labels.append(svg.Text(
            x=left_edge, y=cy - R_RING - 10, text="Both",
            text_anchor="start", font_size=11, fill="#888", font_style="italic"
        ))

    for tid, (gx, gy) in cloud_pos.items():
        t         = topics[tid]
        n         = len(t["targets"])
        blob_r    = R_CLOUD + 5 + n * 2 # REDUCED formula
        stid      = safe_id(tid)
        avg_s     = t["avg_sentiment"]
        avg_s_raw = t["avg_sentiment_raw"]

        clouds += [
            svg.Circle(cx=gx, cy=gy, r=blob_r,
                       fill=color_for(avg_s), opacity=0.18,
                       stroke=color_for(avg_s), stroke_width=1.2, stroke_dasharray="4,3"),
            svg.Text(x=gx, y=gy - blob_r - 6, text=str(tid),
                     text_anchor="middle", font_size=10, fill="#444", font_weight="bold"),
            svg.Text(x=gx, y=gy, text=f"{avg_s_raw:+.2f}",
                     id=f"group_sent_{stid}", class_="group-sent",
                     text_anchor="middle", dominant_baseline="central", font_size=12,
                     fill=color_for(avg_s), font_weight="bold")
        ]

    for pid, topics_dict in pid_to_topics.items():
        px, py, p = person_pos[pid]
        spid = safe_id(pid)
        for top_id, (p_sent, p_raw) in topics_dict.items():
            if top_id not in cloud_pos: 
                continue
            gx, gy = cloud_pos[top_id]
            stop = safe_id(top_id)
            stroke_w = max(0.8, abs(p_sent) * 10)
            col = color_for(p_sent)

            t = topics[top_id]
            n = len(t["targets"])
            blob_r = R_CLOUD + 5 + n * 2

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
        base = dict(id=f"node_{stid}", fill=fill, opacity="1", stroke="#999", stroke_width=1.0)

        if data["target_type"] == "discussion":
            sz = 10 # REDUCED from 14
            c1, s1 = 0.951, 0.309 
            c2, s2 = 0.588, 0.809 
            pts = f"{tx},{ty-sz} {tx+sz*c1},{ty-sz*s1} {tx+sz*c2},{ty+sz*s2} {tx-sz*c2},{ty+sz*s2} {tx-sz*c1},{ty-sz*s1}"
            target_nodes.append(DataPolygon(data_fill=fill, points=pts, **base))
        else:
            sz = 8 # REDUCED from 11
            target_nodes.append(DataRect(data_fill=fill, x=tx-sz, y=ty-sz, width=sz*2, height=sz*2, **base))

        val_str = f"{data['avg_sentiment_raw']:+.1f}".replace("+0.0", "0.0")

        target_nodes.append(svg.Text(
            x=tx, y=ty, text=val_str, id=f"target_text_bg_{stid}", class_="target-sent",
            font_size=7.5, fill="none", stroke="rgba(255, 255, 255, 0.8)", stroke_width=2, stroke_linejoin="round",
            text_anchor="middle", dominant_baseline="central", style="pointer-events:none;" 
        ))
        target_nodes.append(svg.Text(
            x=tx, y=ty, text=val_str, id=f"target_text_fg_{stid}", class_="target-sent",
            font_size=7.5, fill="#111", font_weight="bold",
            text_anchor="middle", dominant_baseline="central", style="pointer-events:none;"
        ))

    for pid, (px, py, p) in person_pos.items():
        role      = p.get("role", "")
        honest    = p.get("honesty", True)
        has_crown = role in ("Committee Chair", "Vice Chair")
        spid      = safe_id(pid)

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
            x=px+10, y=py+3, text=f"{name} ({p_sent:+.2f})",
            font_size=9, fill="#333", id=f"text_{spid}", class_="person-label"
        ))

        ring_layer.append(DataG(
            id=f"person_{spid}",
            elements=g_els,
            style="cursor:pointer",
            data_targets=";".join(connected_targets),
            data_topics=";".join(connected_topics),
            data_pid=spid,
        ))

    centre_layer += [
        svg.Circle(cx=cx, cy=cy, r=R_RING,
                   fill="none", stroke="#ddd", stroke_dasharray="3,3"),
        svg.Circle(cx=cx, cy=cy, r=R_CENTER,
                   fill="url(#g_mean)", stroke="#333", stroke_width=1.5),
        svg.Text(x=cx, y=cy, text=f"{mean_val:+.2f}",
                 fill=text_ink(mean_val), text_anchor="middle",
                 dominant_baseline="central", font_size=12, font_weight="bold"),
    ]

    all_el  = bg + legend_layer + zone_labels + clouds + edges + target_nodes + ring_layer + centre_layer
    svg_str = svg.SVG(height=H, width=W, elements=all_el).as_str()

    _JS = """
    <style>
    body {
       font-family: sans-serif;
       display: flex;
       justify-content: center;
       margin: 0;
       padding-top: 20px; 
    }
    .glow-target {
       filter: drop-shadow(0 0 5px #e74c3c) drop-shadow(0 0 5px #e74c3c);
       stroke: #c0392b !important;
       stroke-width: 2.5px !important;
    }
    </style>
    <script>
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

    # CHANGED: iframe dimensions reduced to prevent scrolling
    visual1 = mo.vstack([mo.md('**Sentiment map**'), mo.vstack([ 
                          mo.iframe(svg_str + _JS, height="700px", width="1200px")], align="center")])
    return (visual1,)


@app.cell
def _():
    ## **Number of trips, plans, discussions, meetings, and topics by focus**
    return


@app.cell
def _(mo, pd):
    try:
        df = pd.read_csv(str(mo.notebook_location() / "data" / "people_participation_summary.csv"))
    except:
        df = pd.read_csv("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/data/people_participation_summary.csv")
    return (df,)


@app.cell
def _(df, mo):

    metrics = ['num_discussions', 'num_meetings', 'num_plans', 'num_topics']
    df[metrics] = df[metrics].fillna(0)

    # Colors
    focus_colors = {
        'fishing': '#2ca02c',  
        'tourism': '#d62728',
        'both': '#17becf',
        'other': '#7f7f7f'
    }
    person_colors = {
        'Carol Limpet': '#ff7f0e',
        'Ed Helpsford': '#9467bd', 
        'Seal': '#8c564b',
        'Simone Kat': '#e377c2',
        'Tante Titan': '#1f77b4',
        'Teddy Goldstein': '#bcbd22'
    }

    # Generate UI Checkboxes
    all_people = sorted(df['people_id'].dropna().unique())
    all_focuses = sorted(df['focus'].dropna().unique())

    people_ui = mo.ui.dictionary({p: mo.ui.checkbox(value=True, label=p) for p in all_people})
    focus_ui = mo.ui.dictionary({f: mo.ui.checkbox(value=True, label=f) for f in all_focuses})
    return focus_colors, focus_ui, metrics, people_ui


@app.cell
def _(df, focus_colors, focus_ui, math, metrics, mo, pd, people_ui):
    # Extract selected values from checkboxes
    selected_people = [p for p, active in people_ui.value.items() if active]
    selected_focuses = [f for f, active in focus_ui.value.items() if active]

    # Filter dataset
    filtered_df = df[
        (df['people_id'].isin(selected_people)) & 
        (df['focus'].isin(selected_focuses))
    ].copy()

    x_labels = ['Discussions', 'Meetings', 'Plans', 'Topics']

    # Calculate totals on filtered data
    if filtered_df.empty:
        totals = pd.DataFrame(columns=metrics)
        max_val = 1 # Prevent division by zero
    else:
        totals = filtered_df.groupby('people_id')[metrics].sum()
        max_val = totals.max().max()
        max_val = max(1, max_val)

    # --- LAYOUT FIXES ---
    # Increased padding to prevent text and bubbles from colliding
    padding_x = 160  
    padding_y = 50   
    right_margin = 180 
    width = 800
    height = max(400, len(selected_people) * 80 + padding_y * 2)

    max_radius = 35 # Maximum circle radius in pixels

    n_x = len(metrics)
    n_y = len(selected_people)

    # Calculate X coordinates safely
    if n_x > 1:
        x_coords = [padding_x + _i * (width - right_margin - padding_x) / (n_x - 1) for _i in range(n_x)]
    else:
        x_coords = [(padding_x + width - right_margin) / 2]

    # Calculate Y coordinates safely
    if n_y > 1:
        y_coords = [padding_y + _i * (height - 2 * padding_y) / (n_y - 1) for _i in range(n_y)]
    else:
        y_coords = [height / 2] if n_y == 1 else []

    svg_elements = []
    svg_elements.append(f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">')

    # CSS Targets the group (.pie-group) to highlight the whole circle at once
    svg_elements.append('''
    <style>
        .pie-slice { transition: opacity 0.2s, stroke-width 0.2s; }
        .pie-group { cursor: pointer; }
        .pie-group:hover .pie-slice { opacity: 0.7; stroke: #333 !important; stroke-width: 1.5 !important; }
    </style>
    ''')

    svg_elements.append(f'<rect width="100%" height="100%" fill="white"/>')

    # Draw X-axis lines and labels (Metrics)
    for _i, x_pos in enumerate(x_coords):
        # Faint vertical guide line (Extended up and down by 45 to clear bubbles)
        svg_elements.append(f'<line x1="{x_pos}" y1="{padding_y - 45}" x2="{x_pos}" y2="{height - padding_y + 45}" stroke="#eeeeee" stroke-width="1"/>')

        # Bottom label (Shifted down by +65 to completely clear the largest bubble)
        svg_elements.append(f'<text x="{x_pos}" y="{height - padding_y + 45}" font-family="sans-serif" font-size="12" font-weight="bold" text-anchor="middle">{x_labels[_i]}</text>')

    # Draw Y-axis lines and labels (People)
    for _i, person in enumerate(selected_people):
        y_pos = y_coords[_i]
        # Faint horizontal guide line (Extended left by 45 to clear bubbles)
        svg_elements.append(f'<line x1="{padding_x - 45}" y1="{y_pos}" x2="{width - right_margin + 20}" y2="{y_pos}" stroke="#dddddd" stroke-dasharray="4" stroke-width="1"/>')

        # Left label (Shifted left by -50 to completely clear the largest bubble)
        svg_elements.append(f'<text x="{padding_x - 50}" y="{y_pos + 4}" font-family="sans-serif" font-size="12" font-weight="bold" text-anchor="end">{person}</text>')

    def create_pie_slice(cx, cy, _r, start_angle, end_angle, color):
        if end_angle - start_angle >= 2 * math.pi - 0.0001:
            return f'<circle class="pie-slice" cx="{cx}" cy="{cy}" r="{_r}" fill="{color}" stroke="white" stroke-width="0.5" />'

        x1 = cx + _r * math.cos(start_angle)
        y1 = cy + _r * math.sin(start_angle)
        x2 = cx + _r * math.cos(end_angle)
        y2 = cy + _r * math.sin(end_angle)

        large_arc = 1 if end_angle - start_angle > math.pi else 0
        path = f'M {cx} {cy} L {x1} {y1} A {_r} {_r} 0 {large_arc} 1 {x2} {y2} Z'

        return f'<path class="pie-slice" d="{path}" fill="{color}" stroke="white" stroke-width="0.5" />'

    # Plot the Data
    for _i, person in enumerate(selected_people):
        y_pos = y_coords[_i]
        for _j, metric in enumerate(metrics):
            x_pos = x_coords[_j]

            if person not in totals.index:
                continue

            person_data = filtered_df[filtered_df['people_id'] == person]
            values = person_data[metric].values
            labels = person_data['focus'].values

            mask = values > 0
            values = values[mask]
            labels = labels[mask]

            if len(values) > 0:
                total_val = values.sum()

                # Area proportional mapping (sqrt of value)
                _r = max_radius * math.sqrt(total_val / max_val)

                # Unified tooltip
                metric_clean = x_labels[_j]
                tooltip_lines = [f"{person} | {metric_clean}", f"Total: {int(total_val)}", "---"]
                for val, label in zip(values, labels):
                    tooltip_lines.append(f"{label.capitalize()}: {int(val)}")

                title_text = "&#10;".join(tooltip_lines)

                svg_elements.append(f'<g class="pie-group"><title>{title_text}</title>')

                start_angle = 0
                for val, label in zip(values, labels):
                    angle = (val / total_val) * 2 * math.pi
                    end_angle = start_angle + angle
                    color = focus_colors.get(label, '#000000')

                    svg_elements.append(create_pie_slice(x_pos, y_pos, _r, start_angle, end_angle, color))
                    start_angle = end_angle

                svg_elements.append('</g>')

    # Legends
    legend_x = width - right_margin + 30
    legend_y = padding_y

    # Focus Legend
    if len(selected_focuses) > 0:
        svg_elements.append(f'<text x="{legend_x}" y="{legend_y}" font-family="sans-serif" font-size="14" font-weight="bold">Focus</text>')
        legend_y += 20
        for focus in selected_focuses:
            color = focus_colors.get(focus, '#000000')
            svg_elements.append(f'<rect x="{legend_x}" y="{legend_y-10}" width="15" height="15" fill="{color}"/>')
            svg_elements.append(f'<text x="{legend_x+25}" y="{legend_y+2}" font-family="sans-serif" font-size="12">{focus.capitalize()}</text>')
            legend_y += 20

    # Size Legend (shows proportional area sizes)
    if max_val > 0 and len(selected_people) > 0:
        legend_y += 20
        svg_elements.append(f'<text x="{legend_x}" y="{legend_y}" font-family="sans-serif" font-size="14" font-weight="bold">Total Items</text>')
        legend_y += 45

        example_vals = [max_val, max_val/2, max_val/4] if max_val >= 4 else [max_val, 1]
        example_vals = sorted(list(set([max(1, int(v)) for v in example_vals])), reverse=True)

        for val in example_vals:
            _r = max_radius * math.sqrt(val / max_val)
            svg_elements.append(f'<circle cx="{legend_x + 30}" cy="{legend_y}" r="{_r}" fill="#dddddd" stroke="white" stroke-width="1"/>')
            svg_elements.append(f'<text x="{legend_x + 50 + max_radius}" y="{legend_y + 4}" font-family="sans-serif" font-size="12">{val}</text>')
            legend_y += _r + max_radius + 5

    svg_elements.append('</svg>')

    visual3 = mo.vstack([
            mo.md('**Number of topics, trips, discussions, meetings**'),
            mo.hstack([
                mo.Html("\n".join(svg_elements)),
                mo.vstack([
                    mo.vstack([mo.md("**Select People:**"), people_ui]),
                    mo.vstack([mo.md("**Select Focus Types:**"), focus_ui])
            ])
        ])


    ])
    return (visual3,)


@app.cell
def _(mo, visual1, visual3):
    mo.ui.tabs({
        "Sentiment map": visual1,
        "Number of topics, trips, discussions, meetings" :visual3, 
    })
    return


if __name__ == "__main__":
    app.run()
