import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import math

    return math, mo, pd


@app.cell
async def _():
    import micropip
    await micropip.install("svg.py")
    import svg

    return


@app.cell
def _():
    ### Code for the CSV file, not on marimo online as sqlglot/duckdb doesn't work online (I blame pyodide)
    # Number of trips, plans, discussions, meetings, and topics for every people in the journalist data
    # nums = duckdb.sql("""
    #     with
    #     topics as ( 
    #         select a.*,
    #             case when topic_id in ('new_crane_lomark',
    #                                     'deep_fishing_dock',
    #                                     'fish_vacuum',
    #                                     'affordable_housing',
    #                                     'low_volume_crane'
    #             ) then 'fishing'
    #             when topic_id in ('expanding_tourist_wharf',
    #                                     'marine_life_deck',
    #                                     'heritage_walking_tour',
    #                                     'waterfront_market',
    #                                     'statue_john_smoth',
    #                                     'concert'
    #                 ) then 'tourism'
    #             when topic_id in ('seafood_festival') then 'both'
    #             else 'other' end as focus
    #         from database_jour.topics a ),                      
    #     num_discussions as (
    #         select people_id, focus,
    #                count(distinct a.discussion_id) as num_discussions,
    #         from database_jour.discussion_people_participations a,
    #             database_jour.discussion_topics b,
    #             topics c
    #         where a.discussion_id = b.discussion_id and b.topic_id = c.topic_id
    #         group by people_id, focus
    #     ),
    #     num_plans as (
    #         select people_id, focus,
    #                count(distinct a.plan_id) as num_plans,
    #         from database_jour.plan_people_participations a,
    #             database_jour.plan_topics b,
    #             topics c
    #         where a.plan_id = b.plan_id and b.topic_id = c.topic_id
    #         group by people_id, focus
    #     ), 
    #     num_meetings as (
    # select people_id, focus_fixed as focus, count(distinct meeting_id) as num_meetings
    #         from (
    #         select *, 
    #                case when contains(focus_list, 'fishing') and contains(focus_list, 'tourism') then 'both'
    #                         when contains(focus_list, 'fishing') then 'fishing'
    #                         when contains(focus_list, 'tourism') then 'tourism'
    #                         else focus_list end as focus_fixed
    #          from (
    #         select people_id, meeting_id, listagg(distinct focus) as focus_list
    #         from (
    #                 select people_id, a.discussion_id as item_id, focus
    #                 from database_jour.discussion_people_participations a
    #                     left join database_jour.discussion_topics b
    #                     on a.discussion_id = b.discussion_id
    #                     left join topics c
    #                     on b.topic_id = c.topic_id
    #                 union all
    #                 select people_id, a.plan_id as item_id, coalesce(focus, 'fishing') as focus
    #                 from database_jour.plan_people_participations a
    #                     left join database_jour.plan_topics b
    #                     on a.plan_id = b.plan_id
    #                     left join topics c
    #                     on b.topic_id = c.topic_id
    #             ) a,
    #             (
    #                 select discussion_id as item_id, meeting_id, 'discussion' as item_type 
    #                 from database_jour.meeting_discussions
    #                 union all
    #                 select plan_id as item_id, meeting_id, 'plan' as item_type
    #                 from database_jour.meeting_plans    
    #             ) b
    #         where a.item_id = b.item_id 
    #         group by people_id, meeting_id ))
    #         group by people_id, focus_fixed
    #         order by people_id, focus_fixed
    #     ),
    #     num_topics as (
    #         select people_id, focus,
    #                count(distinct topic_id) as num_topics 
    #         from (
    #                 select people_id, b.topic_id, focus
    #                 from database_jour.discussion_people_participations a,
    #                     database_jour.discussion_topics b,
    #                    topics c
    #                 where a.discussion_id = b.discussion_id and b.topic_id = c.topic_id
    #                 union all
    #                 select people_id, b.topic_id, focus
    #                 from database_jour.plan_people_participations a,
    #                     database_jour.plan_topics b,
    #                     topics c
    #                 where a.plan_id = b.plan_id and b.topic_id = c.topic_id
    #         ) 
    #         group by people_id, focus      
    #     ),
    #     num_trips as (
    #         select people_id,
    #                 count(distinct trip_id) as num_trips,
    #                 'both' as focus 
    #         from database_jour.trip_people a   
    #         group by people_id
    #     )
    #     select *
    #     from num_discussions
    #     full outer join num_meetings using (people_id, focus)
    #     full outer join num_plans using (people_id, focus)
    #     full outer join num_topics using (people_id, focus)
    # """).to_df()

    # nums.fillna(0)
    return


@app.cell
def _(pd):
    try:
        df = pd.read_csv("data/people_participation_summary.csv")
    except:
        df = pd.read_csv("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/data/people_participation_summary.csv")
    return (df,)


@app.cell
def _(df):
    df
    return


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
def _():
    # # Extract selected values from checkboxes
    # selected_people = [p for p, active in people_ui.value.items() if active]
    # selected_focuses = [f for f, active in focus_ui.value.items() if active]

    # # Filter dataset
    # filtered_df = df[
    #     (df['people_id'].isin(selected_people)) & 
    #     (df['focus'].isin(selected_focuses))
    # ].copy()

    # # Calculate totals on filtered data
    # if filtered_df.empty:
    #     totals = pd.DataFrame(columns=metrics)
    #     max_y = 5 # Default scale if no data
    # else:
    #     totals = filtered_df.groupby('people_id')[metrics].sum()
    #     max_y = totals.max().max()
    #     max_y = max(5, math.ceil(max_y / 5) * 5) # Avoid zero division

    # width = 800
    # # INCREASED: height changed from 500 to 800 to stretch the y-axis
    # height = 800 
    # padding = 80

    # x_labels = ['Discussions', 'Meetings', 'Plans', 'Topics']
    # n_x = len(metrics)
    # x_coords = [padding + i * (width - 2 * padding) / (n_x - 1) for i in range(n_x)]

    # def get_y(val):
    #     return height - padding - (val / max_y) * (height - 2 * padding)

    # svg_elements = []
    # svg_elements.append(f'<svg width="{width + 200}" height="{height}" xmlns="http://www.w3.org/2000/svg">')

    # # CSS targets the group (.pie-group) to highlight the whole circle at once
    # svg_elements.append('''
    # <style>
    #     .pie-slice { transition: opacity 0.2s, stroke-width 0.2s; }
    #     .pie-group { cursor: pointer; }
    #     .pie-group:hover .pie-slice { opacity: 0.7; stroke: #333 !important; stroke-width: 1.5 !important; }
    # </style>
    # ''')

    # svg_elements.append(f'<rect width="100%" height="100%" fill="white"/>')
    # svg_elements.append(f'<line x1="{padding}" y1="{height - padding}" x2="{width - padding}" y2="{height - padding}" stroke="black" stroke-width="1"/>')
    # svg_elements.append(f'<line x1="{padding}" y1="{padding}" x2="{padding}" y2="{height - padding}" stroke="black" stroke-width="1"/>')

    # # Y-axis ticks
    # for y_val in range(0, int(max_y) + 1, max(1, int(max_y)//5)):
    #     y_pos = get_y(y_val)
    #     svg_elements.append(f'<line x1="{padding-5}" y1="{y_pos}" x2="{width-padding}" y2="{y_pos}" stroke="lightgray" stroke-dasharray="4"/>')
    #     svg_elements.append(f'<text x="{padding-10}" y="{y_pos+4}" font-family="sans-serif" font-size="12" text-anchor="end">{y_val}</text>')

    # # X-axis ticks
    # for i, x_pos in enumerate(x_coords):
    #     svg_elements.append(f'<text x="{x_pos}" y="{height - padding + 20}" font-family="sans-serif" font-size="12" text-anchor="middle">{x_labels[i]}</text>')

    # # Lines
    # for person in totals.index:
    #     color = person_colors.get(person, '#000000')
    #     pts = []
    #     for i, metric in enumerate(metrics):
    #         val = totals.loc[person, metric]
    #         pts.append(f"{x_coords[i]},{get_y(val)}")
    #     pts_str = " ".join(pts)
    #     svg_elements.append(f'<polyline points="{pts_str}" fill="none" stroke="{color}" stroke-width="2"/>')

    # # Pies
    # pie_radius = 12

    # def create_pie_slice(cx, cy, r, start_angle, end_angle, color):
    #     if end_angle - start_angle >= 2 * math.pi - 0.0001:
    #         return f'<circle class="pie-slice" cx="{cx}" cy="{cy}" r="{r}" fill="{color}" stroke="white" stroke-width="0.5" />'

    #     x1 = cx + r * math.cos(start_angle)
    #     y1 = cy + r * math.sin(start_angle)
    #     x2 = cx + r * math.cos(end_angle)
    #     y2 = cy + r * math.sin(end_angle)

    #     large_arc = 1 if end_angle - start_angle > math.pi else 0
    #     path = f'M {cx} {cy} L {x1} {y1} A {r} {r} 0 {large_arc} 1 {x2} {y2} Z'

    #     return f'<path class="pie-slice" d="{path}" fill="{color}" stroke="white" stroke-width="0.5" />'

    # for person in totals.index:
    #     for i, metric in enumerate(metrics):
    #         cx = x_coords[i]
    #         cy = get_y(totals.loc[person, metric])

    #         person_data = filtered_df[filtered_df['people_id'] == person]
    #         values = person_data[metric].values
    #         labels = person_data['focus'].values

    #         mask = values > 0
    #         values = values[mask]
    #         labels = labels[mask]

    #         if len(values) > 0:
    #             total_val = values.sum()

    #             # Unified tooltip
    #             metric_clean = x_labels[i]
    #             tooltip_lines = [f"{person} | {metric_clean}", f"Total: {int(total_val)}", "---"]
    #             for val, label in zip(values, labels):
    #                 tooltip_lines.append(f"{label.capitalize()}: {int(val)}")

    #             title_text = "&#10;".join(tooltip_lines)

    #             svg_elements.append(f'<g class="pie-group"><title>{title_text}</title>')

    #             start_angle = 0
    #             for val, label in zip(values, labels):
    #                 angle = (val / total_val) * 2 * math.pi
    #                 end_angle = start_angle + angle
    #                 color = focus_colors.get(label, '#000000')

    #                 svg_elements.append(create_pie_slice(cx, cy, pie_radius, start_angle, end_angle, color))
    #                 start_angle = end_angle

    #             svg_elements.append('</g>')

    # # Legends
    # legend_x = width - padding + 40
    # legend_y = padding

    # if len(totals.index) > 0:
    #     svg_elements.append(f'<text x="{legend_x}" y="{legend_y}" font-family="sans-serif" font-size="14" font-weight="bold">People</text>')
    #     legend_y += 20
    #     for person in totals.index:
    #         color = person_colors.get(person, '#000000')
    #         svg_elements.append(f'<line x1="{legend_x}" y1="{legend_y-4}" x2="{legend_x+20}" y2="{legend_y-4}" stroke="{color}" stroke-width="2"/>')
    #         svg_elements.append(f'<text x="{legend_x+25}" y="{legend_y}" font-family="sans-serif" font-size="12">{person}</text>')
    #         legend_y += 20

    # if len(selected_focuses) > 0:
    #     legend_y += 20
    #     svg_elements.append(f'<text x="{legend_x}" y="{legend_y}" font-family="sans-serif" font-size="14" font-weight="bold">Focus</text>')
    #     legend_y += 20
    #     for focus in selected_focuses:
    #         color = focus_colors.get(focus, '#000000')
    #         svg_elements.append(f'<rect x="{legend_x}" y="{legend_y-10}" width="15" height="15" fill="{color}"/>')
    #         svg_elements.append(f'<text x="{legend_x+25}" y="{legend_y+2}" font-family="sans-serif" font-size="12">{focus.capitalize()}</text>')
    #         legend_y += 20

    # svg_elements.append('</svg>')

    # mo.vstack([
    #     mo.md("**Select People:**"), people_ui,
    #     mo.md("**Select Focus Types:**"), focus_ui,
    #     mo.Html("\n".join(svg_elements))
    # ])
    return


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
    padding_y = 80   
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
        svg_elements.append(f'<text x="{x_pos}" y="{height - padding_y + 65}" font-family="sans-serif" font-size="12" font-weight="bold" text-anchor="middle">{x_labels[_i]}</text>')

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
    mo.Html("\n".join(svg_elements))

    mo.hstack([
        mo.Html("\n".join(svg_elements)),
        mo.vstack([
            mo.vstack([mo.md("**Select People:**"), people_ui]),
            mo.vstack([mo.md("**Select Focus Types:**"), focus_ui])
        ]),
    ])
    return


if __name__ == "__main__":
    app.run()
