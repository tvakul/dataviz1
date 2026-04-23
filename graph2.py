import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import numpy as np
    from sklearn.neighbors import BallTree
    import svg
    import math
    import json
    import os

    return BallTree, json, math, mo, np, pd, svg


@app.cell
def _(BallTree, np, pd):

    def classify_weighted_knn(df, k=5, max_radius_km=1.0, epsilon=0.001):
        # 1. Split data
        df_comm = df[df['zone'].isin(['commercial', 'residential'])].reset_index(drop=True)
        df_targets = df[df['zone'].isin(['tourism', 'industrial'])].reset_index(drop=True)

        if df_comm.empty or df_targets.empty:
            return pd.DataFrame()

        # 2. Build Tree and Query
        comm_rad = np.radians(df_comm[['lat', 'lon']])
        target_rad = np.radians(df_targets[['lat', 'lon']])
        tree = BallTree(target_rad, metric='haversine')

        earth_radius_km = 6371.0
        radius_rad = max_radius_km / earth_radius_km
        distances, indices = tree.query(comm_rad, k=k, return_distance=True)

        # 3. Collect Valid Neighbors and Calculate Inverse Distance Weight
        results = []
        for i, (dist_array, idx_array) in enumerate(zip(distances, indices)):
            for dist, idx in zip(dist_array, idx_array):
                if dist <= radius_rad:
                    dist_km = dist * earth_radius_km
                    weight = 1.0 / (dist_km + epsilon) # Add epsilon to avoid zero division

                    results.append({
                        'commercial_id': df_comm.iloc[i]['place_id'],
                        'target_zone': df_targets.iloc[idx]['zone'],
                        'weight': weight
                    })

        df_neighbors = pd.DataFrame(results)
        if df_neighbors.empty:
            return pd.DataFrame(columns=['commercial_id', 'predicted_zone', 'weighted_score'])

        # 4. Sum the weights grouped by commercial_id and target_zone
        weighted_scores = (df_neighbors.groupby(['commercial_id', 'target_zone'])['weight']
                                       .sum()
                                       .reset_index(name='weighted_score'))

        # 5. Sort by highest weighted score to determine majority
        df_majority = (weighted_scores.sort_values(['commercial_id', 'weighted_score'], ascending=[True, False])
                                      .drop_duplicates(subset=['commercial_id'], keep='first')
                                      .rename(columns={'target_zone': 'predicted_zone'}))

        return df_majority.reset_index(drop=True)

    return (classify_weighted_knn,)


@app.cell
def _(mo, pd):
    # places_edited = duckdb.sql("""
    #     select place_id, 
    #            case when zone = 'residential' and name = 'Paakland Elementary' then 'tourism'
    #                 when zone = 'residential' and name in ('Waveside Townhomes', 'Tidewater Flats') then 'industrial'
    #                 when name = 'Tropics Environmental Hub' then 'tourism'
    #                 else zone end as zone, lat, lon
    #     from database_jour.places
    # """).to_df()

    # places_edited.to_json("places_edited.json", index=False)

    try:
        places_edited = pd.read_json(str(mo.notebook_location() / "data" / "places_edited.json"))
    except:
        places_edited = pd.read_json("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/data/places_edited.json")
    places_edited
    return (places_edited,)


@app.cell
def _(
    classify_weighted_knn,
    knn_dist_slider,
    knn_num_slider,
    pd,
    places_edited,
):
    remapped_location = pd.concat([places_edited.loc[places_edited['zone'].isin(['industrial', 'tourism'])],
    classify_weighted_knn(places_edited, max_radius_km=knn_dist_slider.value, k=knn_num_slider.value).rename(
        columns={'predicted_zone': 'zone', 'commercial_id': 'place_id'}
    )
    ], ignore_index=True).drop_duplicates(subset=['place_id'], keep='first')[['place_id', 'zone']]

    remapped_location
    return (remapped_location,)


@app.cell(hide_code=True)
def _(mo, pd):
    # time_trip_spend = duckdb.sql("\""
    #     select a.*, b.*, c.*, d.people_id 
    #     from 
    #       database_jour.trips a,
    #      database_jour.trip_places b,
    #         database_jour.places c,
    #         database_jour.trip_people d
    #         where a.trip_id = b.trip_id and b.place_id = c.place_id and a.trip_id = d.trip_id
    #     order by a.trip_id, time
    # "\"").to_df()

    # # Create index column by trip_id and time
    # time_trip_spend["index"] = time_trip_spend.groupby("trip_id").cumcount()
    # time_trip_spend["index_lead"] = time_trip_spend.groupby("trip_id")["index"].shift(-1).fillna(9999).astype(int)

    # # Convert start_time, end_time, time to datetime
    # time_trip_spend["date"] = time_trip_spend["date"].str.replace("0040","2040")
    # time_trip_spend["start_time"] = pd.to_datetime(time_trip_spend["date"] + " " + time_trip_spend["start_time"])
    # time_trip_spend["end_time"] = pd.to_datetime(time_trip_spend["date"] + " " + time_trip_spend["end_time"])
    # time_trip_spend["time"] = pd.to_datetime(time_trip_spend["time"].str.replace("0040","2040"))
    # time_trip_spend["time_lead"] = time_trip_spend.groupby("trip_id")["time"].shift(-1).fillna(time_trip_spend["time"])

    # #  1. Shift to get adjacent location times
    # time_trip_spend["time_prev"] = time_trip_spend.groupby("trip_id")["time"].shift(1)
    # time_trip_spend["time_lead"] = time_trip_spend.groupby("trip_id")["time"].shift(-1)

    # # 2. Calculate the bounding time intervals
    # # Divided by 2 for the shared transit times
    # gap_start = time_trip_spend["time"] - time_trip_spend["start_time"]
    # gap_prev = (time_trip_spend["time"] - time_trip_spend["time_prev"]) / 2

    # gap_end = time_trip_spend["end_time"] - time_trip_spend["time"]
    # gap_lead = (time_trip_spend["time_lead"] - time_trip_spend["time"]) / 2

    # # Handle overnight boundary for the end gap if your times cross midnight
    # gap_end = gap_end.mask(gap_end < pd.Timedelta(0), gap_end + pd.Timedelta(days=1))

    # # 3. Allocate the time halves
    # # If gap_prev is NaT (first row), it falls back to the full gap_start
    # time_trip_spend["half_before"] = gap_prev.fillna(gap_start)

    # # If gap_lead is NaT (last row), it falls back to the full gap_end
    # time_trip_spend["half_after"] = gap_lead.fillna(gap_end)

    # # 4. Final Calculation
    # time_trip_spend["time_spend"] = time_trip_spend["half_before"] + time_trip_spend["half_after"]

    # # Clean up intermediate columns
    # time_trip_spend = time_trip_spend.drop(columns=["time_prev", "time_lead", "half_before", "half_after"])
    # time_trip_spend[["trip_id", "people_id", "place_id", "name", "time", "time_spend", "zone", "zone_detail"]]


    _dtypes_tts = {
        "trip_id": "object",
        "date": "object",
        "start_time": "datetime64[ns]",
        "end_time": "datetime64[ns]",
        "trip_id_1": "object",
        "place_id": "object",
        "time": "datetime64[ns]",
        "place_id_1": "object",
        "name": "object",
        "lat": "float64",
        "lon": "float64",
        "zone": "object",
        "zone_detail": "object",
        "people_id": "object",
        "index": "int64",
        "index_lead": "int64",
        "time_spend": "timedelta64[ns]"
    }
    try:
        time_trip_spend = pd.read_json(str(mo.notebook_location() / "data" / "time_trip_spend.json"), dtype=_dtypes_tts)
    except:
        time_trip_spend = pd.read_json("https://raw.githubusercontent.com/tvakul/dataviz1/refs/heads/main/data/time_trip_spend.json", dtype=_dtypes_tts)
    time_trip_spend
    return (time_trip_spend,)


@app.cell
def _(pd, remapped_location, time_trip_spend):
    time_location_remapped = pd.merge(time_trip_spend[["trip_id", "date", "people_id", "place_id", "name", "time", "time_spend", "zone", 'lat', 'lon']], remapped_location.rename(columns={"zone": "zone_remapped"}),
                on="place_id", how="left").fillna({"zone_remapped": "other"})
    time_location_remapped
    return (time_location_remapped,)


@app.cell(hide_code=True)
def _(time_location_remapped):
    # Group by people_id and zone_remapped to calculate total visits by trip_id and place_id
    _tmp = time_location_remapped.copy()
    _tmp['visit_id'] = _tmp['trip_id'].astype(str) + '_' + _tmp['place_id'].astype(str) + '_' + _tmp['place_id'].astype(str)
    people_zone_summary = _tmp.groupby(['people_id', 'zone_remapped']).agg(
        total_visits=('visit_id', 'nunique'),   
    ).reset_index()

    people_zone_summary = people_zone_summary[people_zone_summary['zone_remapped'].isin(['industrial', 'tourism'])]


    people_zone_summary_delta = people_zone_summary.pivot(index='people_id', columns='zone_remapped', values='total_visits').fillna(0).reset_index()
    people_zone_summary_delta['delta'] = people_zone_summary_delta['industrial'] - people_zone_summary_delta['tourism']
    people_zone_summary_delta.sort_values('delta', ascending=False)
    return (people_zone_summary_delta,)


@app.cell
def _(pd, time_location_remapped):
    people_timespend_summary = time_location_remapped.groupby(["people_id", "zone_remapped"]).agg({"time_spend": "sum", "trip_id": "nunique"}).reset_index()

    # Filter out zone_remapped = 'other'
    people_timespend_summary_filtered = people_timespend_summary[people_timespend_summary['zone_remapped'].isin(['industrial', 'tourism'])].copy()

    # Get the delta time spend between different zones for each people_id
    delta_time_spend = people_timespend_summary_filtered.pivot(index='people_id', columns='zone_remapped', values='time_spend').fillna(pd.Timedelta(0))
    delta_time_spend['delta'] = delta_time_spend.get('industrial', pd.Timedelta(0)) - delta_time_spend.get('tourism', pd.Timedelta(0))

    delta_time_spend
    return (delta_time_spend,)


@app.cell
def _(mo):
    knn_dist_slider = mo.ui.slider(start=0, stop=5, step=0.1, value=1.5, show_value=True)
    knn_num_slider = mo.ui.slider(start=0, stop=10, step=1, value=5, show_value=True)
    mode_dropdown = mo.ui.dropdown(options=['time_spend', 'visits'], value='visits')
    show_others = mo.ui.checkbox(value=False, label="Show 'Others'")
    return knn_dist_slider, knn_num_slider, mode_dropdown, show_others


@app.cell
def _(time_location_remapped):
    time_location_remappd2 = time_location_remapped.copy();
    time_location_remappd2.groupby(['people_id', 'lat', 'lon', 'trip_id']).agg({'time_spend': 'sum'}).reset_index().rename(columns={'place_id': 'time_spend'})
    return


@app.cell
def _(
    delta_time_spend,
    mode_dropdown,
    people_zone_summary_delta,
    time_location_remapped,
):
    if mode_dropdown.value == 'visits':
        graph_2_1_data = people_zone_summary_delta
        graph_2_2_data = time_location_remapped.groupby(['people_id', 'date', 'zone_remapped', 'trip_id', 'place_id']).size().reset_index(name='num_visits')
        graph_2_2_data['time_spend'] = 0
    else:
        graph_2_1_data = delta_time_spend.reset_index()
        temp_df = time_location_remapped.copy()
        if temp_df['time_spend'].dtype == 'timedelta64[ns]':
            temp_df['time_hr'] = temp_df['time_spend'].dt.total_seconds() / 3600
        else:
            temp_df['time_hr'] = temp_df['time_spend']

        graph_2_2_data = temp_df.groupby(['people_id', 'date', 'zone_remapped', 'trip_id', 'place_id'])['time_hr'].sum().reset_index(name='time_spend')
        graph_2_2_data['num_visits'] = 1
    return graph_2_1_data, graph_2_2_data


@app.cell
def _(graph_2_1_data):
    graph_2_1_data
    return


@app.cell
def _(graph_2_1_data):
    graph_2_1_data.to_json('data/graph_2_1_data.json')
    return


@app.cell
def _(json):
    try:
        with open("data/oceanus_map.geojson", "r") as f:
            oceanus_geojson = json.load(f)
    except:
         # Fallback or error
         oceanus_geojson = None
    return (oceanus_geojson,)


@app.cell
def _(pd, time_location_remapped):
    # Prepare data for C6 (Map)
    visit_counts = time_location_remapped.groupby(['place_id', 'zone_remapped', 'lat', 'lon']).size().reset_index(name='count')
    total_time = time_location_remapped.groupby(['place_id', 'zone_remapped', 'lat', 'lon'])['time_spend'].sum().dt.total_seconds() / 3600
    visit_data = pd.merge(visit_counts, total_time.reset_index(name='hours'), on=['place_id', 'zone_remapped', 'lat', 'lon'])
    return (visit_data,)


@app.cell
def _(visit_data):
    visit_data
    return


@app.cell
def _(svg):
    def draw_boat(x, y, scale=1.0):
        return svg.G(transform=f"translate({x}, {y}) scale({scale})", elements=[
            svg.Path(d="M 0 0 L 100 0 L 80 30 L 20 30 Z", fill="white", stroke="#333", stroke_width=2),
            svg.Path(d="M 10 10 L 90 10", stroke="#3498db", stroke_width=4),
            svg.Line(x1=50, y1=0, x2=50, y2=-60, stroke="#333", stroke_width=3),
            svg.Path(d="M 50 -55 Q 80 -30 50 -5 L 50 -55 Z", fill="#eee", stroke="#333", stroke_width=1),
        ])

    def draw_rowboat(x, y, scale=0.5, color="orange", name=""):
        return svg.G(id=f"boat_{name.replace(' ', '_')}", class_="member-node", transform=f"translate({x}, {y}) scale({scale})", style="cursor:pointer", elements=[
            svg.Path(d="M 0 0 C 10 20 50 20 60 0 L 50 0 C 45 10 15 10 10 0 Z", fill="#8b4513", stroke="#333"),
            svg.Circle(cx=30, cy=-20, r=10, fill=color),
            svg.Rect(x=20, y=-10, width=20, height=20, fill=color),
            svg.Text(x=30, y=30, text=name, text_anchor="middle", font_size=12, fill="#333", font_weight="bold")
        ])

    def draw_umbrella(x, y, scale=1.0, color="orange", name=""):
        return svg.G(id=f"beach_{name.replace(' ', '_')}", class_="member-node", transform=f"translate({x}, {y}) scale({scale})", style="cursor:pointer", elements=[
            svg.Line(x1=0, y1=0, x2=0, y2=-60, stroke="#333", stroke_width=2),
            svg.Path(d="M -40 -40 Q 0 -80 40 -40 Z", fill="#e74c3c", stroke="#333"),
            svg.Circle(cx=0, cy=-25, r=8, fill=color),
            svg.Rect(x=-8, y=-17, width=16, height=20, fill=color),
            svg.Text(x=0, y=20, text=name, text_anchor="middle", font_size=12, fill="#333", font_weight="bold")
        ])

    def draw_person(x, y, scale=0.8, color="#2c3e50", name=""):
        return svg.G(id=f"person_{name.replace(' ', '_')}", class_="member-node", transform=f"translate({x}, {y}) scale({scale})", style="cursor:pointer", elements=[
            svg.Circle(cx=0, cy=-45, r=12, fill=color),
            svg.Path(d="M -20 0 Q -20 -30 0 -30 Q 20 -30 20 0 Z", fill=color),
            svg.Text(x=0, y=25, text=name, text_anchor="middle", font_size=12, fill="#333", font_weight="bold")
        ])

    return (draw_person,)


@app.cell
def _(
    draw_person,
    fmt,
    graph_2_1_data,
    graph_2_2_data,
    math,
    oceanus_geojson,
    places_edited,
    svg,
    time_location_remapped,
):
    class DataPath(svg.Path):
        def __init__(self, **kwargs):
            self.data_info = kwargs.pop('data_info', '')
            super().__init__(**kwargs)
        def as_str(self):
            s = super().as_str()
            return s.replace("/>", f' data-info="{self.data_info}"/>', 1)

    class DataPolygon(svg.Polygon):
        def __init__(self, **kwargs):
            self.data_info = kwargs.pop('data_info', '')
            super().__init__(**kwargs)
        def as_str(self):
            s = super().as_str()
            return s.replace("/>", f' data-info="{self.data_info}"/>', 1)

    class DataG(svg.G):
        def __init__(self, **kwargs):
            self.data_info = kwargs.pop('data_info', '')
            super().__init__(**kwargs)
        def as_str(self):
            s = super().as_str()
            return s.replace("<g ", f'<g data-info="{self.data_info}" ', 1)

    class DataCircle(svg.Circle):
        def __init__(self, **kwargs):
            self.data_info = kwargs.pop('data_info', '')
            super().__init__(**kwargs)
        def as_str(self):
            s = super().as_str()
            return s.replace("/>", f' data-info="{self.data_info}"/>', 1)

    def format_duration(hours):
        h = float(hours)
        days = int(h // 24)
        rem_h = h % 24
        mins = int((rem_h * 60) % 60)
        hh = int(rem_h)
        time_str = f"{hh:02d} hr :{mins:02d} mi"
        if days == 0:
            return time_str
        elif days == 1:
            return f"1 day, {time_str}"
        else:
            return f"{days} days, {time_str}"

    def create_dashboard(mode='visits', show_others=False):
        W, H = 1200, 900
        els = []

        # Dynamic Titles
        if mode == 'visits':
            title_c5 = "Visit bias"
            title_c6 = "Visit map"
            title_c7 = "Trip visit split"
        else:
            title_c5 = "Time spend bias"
            title_c6 = "Time spend map"
            title_c7 = "Trip time split"

        def fmt(v):
            return f"{v:.0f}" if mode == 'visits' else format_duration(v)

        # Mapping industrial -> fishing, hybrid -> both
        focus_colors = {
            'fishing': '#2ca02c',  
            'tourism': '#d62728',
            'other': '#7f7f7f'
        }
        name_map = {'industrial': 'fishing', 'tourism': 'tourism', 'other': 'other'}

        # --- C5: Shoreline Bias ---
        c5_x, c5_y, c5_w, c5_h = 50, 50, 1100, 310
        els.append(svg.Rect(x=c5_x, y=c5_y, width=c5_w, height=c5_h, fill="#f8f9f9", stroke="#ccc", rx=10))
        els.append(svg.Text(x=c5_x+20, y=c5_y+30, text=title_c5, font_size=20, font_weight="bold", fill="#2c3e50"))

        data_c5 = graph_2_1_data.copy()
        pos_col = 'delta'
        if mode == 'time_spend':
            data_c5['delta_val'] = data_c5['delta'].apply(lambda x: x.total_seconds() / 3600 if hasattr(x, 'total_seconds') else x)
            pos_col = 'delta_val'

        min_d = min(0, data_c5[pos_col].min()) - 2
        max_d = max(0, data_c5[pos_col].max()) + 2
        range_d = max_d - min_d or 1
        border_ratio = (0 - min_d) / range_d
        border_x = c5_x + border_ratio * c5_w

        # Draw Sea and Shore with labels
        els.append(svg.Rect(x=c5_x, y=c5_y+80, width=border_ratio*c5_w, height=160, fill="#fdedec", opacity=0.8))
        els.append(svg.Rect(x=border_x, y=c5_y+80, width=(1-border_ratio)*c5_w, height=160, fill="#2ca02c", opacity=0.15))

        # Labels for the regions - Moved to the top to avoid overlap
        els.append(svg.Text(x=c5_x + 15, y=c5_y+100, text="TOURISM BIAS (-)", text_anchor="start", font_size=14, font_weight="bold", fill="#943126"))
        els.append(svg.Text(x=c5_x+c5_w - 15, y=c5_y+100, text="(+) FISHING BIAS", text_anchor="end", font_size=14, font_weight="bold", fill="#186a3b"))

        # Border line
        els.append(svg.Line(x1=border_x, y1=c5_y+80, x2=border_x, y2=c5_y+240, stroke="#95a5a6", stroke_width=1, stroke_dasharray="4"))
        els.append(svg.Text(x=border_x, y=c5_y+75, text="NEUTRAL", text_anchor="middle", font_size=11, font_weight="bold", fill="#7f8c8d"))

        # Person icons - Smaller and cleaner
        for i, (_, row) in enumerate(data_c5.iterrows()):
            val = row[pos_col]
            x = c5_x + ((val - min_d) / range_d) * c5_w
            pid_safe = row['people_id'].replace(' ', '_')
            y_off = (i % 2) * 55

            els.append(svg.G(class_=f"clickable-person pid-{pid_safe}", style="cursor:pointer", elements=[
                draw_person(x, c5_y+215 - y_off, 0.7, name=row['people_id'])
            ]))

        # X-Axis at the bottom
        ax_y = c5_y + c5_h - 40
        els.append(svg.Line(x1=c5_x+20, y1=ax_y, x2=c5_x+c5_w-20, y2=ax_y, stroke="#7f8c8d", stroke_width=1.5))
        # Ticks for min, 0, max
        for v in [min_d + 2, 0, max_d - 2]:
            tx = c5_x + ((v - min_d) / range_d) * c5_w
            els.append(svg.Line(x1=tx, y1=ax_y-5, x2=tx, y2=ax_y+5, stroke="#7f8c8d"))
            els.append(svg.Text(x=tx, y=ax_y+18, text=f"{v:.1f}", text_anchor="middle", font_size=10, fill="#7f8c8d"))
        els.append(svg.Text(x=c5_x+100, y=ax_y-8, text="← PRO-TOURISM (-)", text_anchor="start", font_size=11, font_weight="bold", fill="#d62728"))
        els.append(svg.Text(x=c5_x+c5_w-100, y=ax_y-8, text="PRO-FISHING (+) →", text_anchor="end", font_size=11, font_weight="bold", fill="#2ca02c"))

        # --- C6: Map ---
        c6_x, c6_y, c6_w, c6_h = 50, 420, 525, 450
        els.append(svg.Rect(x=c6_x, y=c6_y, width=c6_w, height=c6_h, fill="#fdfefe", stroke="#ccc", rx=10))
        els.append(svg.Text(x=c6_x+20, y=c6_y+30, text=title_c6, font_size=18, font_weight="bold", fill="#2c3e50"))

        def project(lon_val, lat_val):
            # In this dataset, 'lat' (~-165) is actually Longitude (X)
            # and 'lon' (~39) is actually Latitude (Y)
            # Standard Orientation: X is horizontal (-166 range), Y is vertical (39 range)
            x_min, x_max = -166.3, -164.2
            y_min, y_max = 38.7, 39.9
            
            x_pct = (lon_val - x_min) / (x_max - x_min)
            y_pct = (lat_val - y_min) / (y_max - y_min)
            
            px = c6_x + 40 + x_pct * (c6_w - 80)
            py = c6_y + c6_h - 40 - y_pct * (c6_h - 80) # Y starts from bottom in our pct
            return px, py

        if oceanus_geojson:
            for feat in oceanus_geojson['features']:
                if feat['properties'].get('Name') in ['Suna Island', 'Thalassa Retreat']:
                    if feat['geometry']['type'] == 'Polygon':
                        for poly in feat['geometry']['coordinates']:
                            # GeoJSON is [c0, c1] -> [-166, 39]. Standard: X=c0, Y=c1
                            pts = " ".join([f"{project(c[0], c[1])[0]},{project(c[0], c[1])[1]}" for c in poly])
                            els.append(svg.Polygon(points=pts, fill="#d5f5e3", stroke="#27ae60", stroke_width=0.5))

        # Pre-process place names if available
        place_names = {}
        if 'name' in places_edited.columns:
            place_names = places_edited.set_index('place_id')['name'].to_dict()

        # Member visits mapping
        visit_summary = graph_2_2_data.groupby(['people_id', 'place_id', 'zone_remapped']).agg({'time_spend': 'sum', 'num_visits': 'sum'}).reset_index()
        visit_summary = visit_summary.merge(places_edited[['place_id', 'lat', 'lon']], on='place_id', how='left')

        mode_col = 'num_visits' if mode == 'visits' else 'time_spend'
        visit_summary['val'] = visit_summary[mode_col]
        max_v = visit_summary['val'].max() or 1

        for _, row in visit_summary.dropna(subset=['lat', 'lon']).iterrows():
            m_zone = name_map.get(row['zone_remapped'], 'other')
            if not show_others and m_zone == 'other': continue
            px, py = project(row['lat'], row['lon'])
            r = (row['val'] / max_v)**0.5 * 15 + 2
            pid_safe = row['people_id'].replace(' ', '_')
            loc_name = place_names.get(row['place_id'], str(row['place_id']))
            info = f"Member Activity | Member: {row['people_id']} | Location: {loc_name} | Focus: {m_zone.capitalize()} | Value: {fmt(row['val'])}"
            els.append(DataCircle(cx=px, cy=py, r=r, fill="#3498db", opacity=0.4, 
                                  class_=f"visit_{pid_safe} map-dot c6-segment", style="display:none",
                                  data_info=info))

        # Total visits (default)
        visit_data_agg = time_location_remapped.groupby(['place_id', 'zone_remapped']).agg({'place_id': 'count', 'time_spend': 'sum'}).rename(columns={'place_id': 'count'}).reset_index()
        visit_data_agg['hours'] = visit_data_agg['time_spend'].dt.total_seconds() / 3600
        visit_data_agg = visit_data_agg.merge(places_edited[['place_id', 'lat', 'lon']], on='place_id', how='left')

        val_metric = 'count' if mode == 'visits' else 'hours'
        max_total = visit_data_agg[val_metric].max() or 1
        total_group = svg.G(id="map_total", class_="map-total", elements=[])
        for _, row in visit_data_agg.dropna(subset=['lat', 'lon']).iterrows():
            px, py = project(row['lat'], row['lon'])
            r = (row[val_metric] / max_total)**0.5 * 12 + 2
            m_zone = name_map.get(row['zone_remapped'], 'other')
            if not show_others and m_zone == 'other': continue

            color = focus_colors.get(m_zone, '#7f7f7f')
            loc_name = place_names.get(row['place_id'], str(row['place_id']))
            info = f"Global Stats | Location: {loc_name} | Total {mode.title()}: {fmt(row[val_metric])} | Category: {m_zone.capitalize()}"
            total_group.elements.append(DataCircle(cx=px, cy=py, r=r, fill=color, opacity=0.3, stroke="#777", stroke_width=0.5,
                                                   class_="c6-segment", style="cursor:pointer",
                                                   data_info=info))
        els.append(total_group)

        # --- C7: Trip split ---
        c7_x, c7_y, c7_w, c7_h = 625, 420, 525, 450
        els.append(svg.Rect(x=c7_x, y=c7_y, width=c7_w, height=c7_h, fill="#fdfefe", stroke="#ccc", rx=10))
        els.append(svg.Text(x=c7_x+20, y=c7_y+30, text=title_c7, font_size=18, font_weight="bold", fill="#2c3e50"))

        cx, cy = c7_x + c7_w*0.5, c7_y + c7_h*0.5 + 20
        inner_r, mid_r, outer_r = 60, 90, 160

        def draw_split(df, pid_safe, is_total=False):
            g = svg.G(id=f"split_{pid_safe}", style="display:none" if not is_total else "display:block", class_="split-info", elements=[])
            if df.empty: return g

            m_data = df.copy()
            m_data['zone_mapped'] = m_data['zone_remapped'].map(name_map).fillna('other')
            if not show_others:
                m_data = m_data[m_data['zone_mapped'] != 'other']

            if m_data.empty:
                g.elements.append(svg.Text(x=cx, y=cy, text="No Matching Activity", text_anchor="middle", font_size=14, fill="#95a5a6", font_style="italic"))
                return g

            metric_col = 'num_visits' if mode == 'visits' else 'time_spend'
            m_data['val'] = m_data[metric_col]

            z_sum = m_data.groupby('zone_mapped')['val'].sum()
            total = z_sum.sum() or 0

            # Center Label Logic
            if mode == 'time_spend':
                center_main = f"{(total/24):.2f}"
                center_sub = "days"
            else:
                center_main = f"{total:.0f}"
                center_sub = "visits"
            
            # Inner Ring (Donut)
            header = "Total Overview" if is_total else "Member Overview"
            donut_info_base = f"{header} | Mode: {mode.title()} | Total: {fmt(total)} | Primary: {z_sum.idxmax().capitalize() if not z_sum.empty else 'N/A'}"
            
            cur_a = -math.pi/2
            if len(z_sum) == 1:
                zone, val = list(z_sum.items())[0]
                pct = (val/total)*100 if total > 0 else 0
                seg_info = f"{donut_info_base} | {zone.capitalize()}: {pct:.1f}%"
                donut_g = DataG(class_="c7-segment", style="cursor:pointer", data_info=seg_info, elements=[])
                donut_g.elements.append(svg.Circle(cx=cx, cy=cy, r=(inner_r + mid_r)/2, fill="none", stroke=focus_colors.get(zone, '#ccc'), stroke_width=(mid_r - inner_r)))
            else:
                donut_g = svg.G(elements=[])
                for zone, val in z_sum.items():
                    swp = (val/max(0.001, total))*2*math.pi
                    if swp < 0.01: continue
                    pct = (val/total)*100 if total > 0 else 0
                    seg_info = f"{donut_info_base} | {zone.capitalize()}: {pct:.1f}%"
                    
                    x1, y1 = cx + inner_r*math.cos(cur_a), cy + inner_r*math.sin(cur_a)
                    x2, y2 = cx + inner_r*math.cos(cur_a+swp), cy + inner_r*math.sin(cur_a+swp)
                    x3, y3 = cx + mid_r*math.cos(cur_a+swp), cy + mid_r*math.sin(cur_a+swp)
                    x4, y4 = cx + mid_r*math.cos(cur_a), cy + mid_r*math.sin(cur_a)
                    arc = 1 if swp > math.pi else 0
                    d = f"M {x1} {y1} A {inner_r} {inner_r} 0 {arc} 1 {x2} {y2} L {x3} {y3} A {mid_r} {mid_r} 0 {arc} 0 {x4} {y4} Z"
                    donut_g.elements.append(DataPath(d=d, fill=focus_colors.get(zone, '#ccc'), stroke="#fff", 
                                                    class_="c7-segment", style="cursor:pointer",
                                                    data_info=seg_info))
                    cur_a += swp
            g.elements.append(donut_g)

            # Outer Ring (Radial Bars) - Group by Trip and Date
            # We want each slice to represent a trip
            trip_data = m_data.groupby(['date', 'trip_id', 'zone_mapped'])['val'].sum().unstack(fill_value=0)
            
            # Ensure all categories exist as columns to avoid KeyError
            categories = ['fishing', 'tourism', 'other']
            trip_data = trip_data.reindex(columns=categories, fill_value=0).reset_index()
            
            # Sort by date then trip_id
            trip_data = trip_data.sort_values(['date', 'trip_id'])
            
            n_t = len(trip_data)
            if n_t > 0:
                a_st = (2*math.pi) / n_t
                m_t_v = trip_data[categories].sum(axis=1).max() or 1
                
                for i, (_, row) in enumerate(trip_data.iterrows()):
                    ang_b = -math.pi/2 + i * a_st
                    cur_br = mid_r + 5
                    t_val = row[categories].sum()
                    
                    date_info = f"Trip Details | ID: {row['trip_id']} | Date: {row['date'].strftime('%Y-%m-%d')} | Total: {fmt(t_val)}"
                    # Create a group for the whole trip slice for easier hovering
                    trip_g = svg.G(elements=[])
                    
                    for zone in categories:
                        val = row.get(zone, 0)
                        h_v = (val/m_t_v)*(outer_r - mid_r - 10)
                        if h_v < 0.5: continue
                        
                        x1, y1 = cx + cur_br*math.cos(ang_b), cy + cur_br*math.sin(ang_b)
                        x2, y2 = cx + cur_br*math.cos(ang_b+a_st*0.9), cy + cur_br*math.sin(ang_b+a_st*0.9)
                        x3, y3 = cx + (cur_br+h_v)*math.cos(ang_b+a_st*0.9), cy + (cur_br+h_v)*math.sin(ang_b+a_st*0.9)
                        x4, y4 = cx + (cur_br+h_v)*math.cos(ang_b), cy + (cur_br+h_v)*math.sin(ang_b)
                        
                        z_info = f"{date_info} | Focus: {zone.capitalize()} | Value: {fmt(val)}"
                        trip_g.elements.append(DataPolygon(
                            points=f"{x1},{y1} {x2},{y2} {x3},{y3} {x4},{y4}", 
                            fill=focus_colors.get(zone, '#ccc'),
                            class_="c7-segment", style="cursor:pointer",
                            data_info=z_info
                        ))
                        cur_br += h_v
                    g.elements.append(trip_g)

            g.elements.append(svg.Text(x=cx, y=cy-5, text=center_main, id=f"t_val_{pid_safe}", text_anchor="middle", font_size=22, font_weight="bold"))
            g.elements.append(svg.Text(x=cx, y=cy+15, text=center_sub, text_anchor="middle", font_size=12, opacity=0.6))
            g.elements.append(svg.Text(x=cx, y=cy+35, text="", id=f"hover_txt_{pid_safe}", class_="hover-tip", text_anchor="middle", font_size=10, fill="#e74c3c", font_weight="bold"))
            return g

        # Total split
        els.append(draw_split(graph_2_2_data, "total", is_total=True))

        members = data_c5['people_id'].unique()
        for member in members:
            pid_safe = member.replace(' ', '_')
            els.append(draw_split(graph_2_2_data[graph_2_2_data['people_id'] == member], pid_safe))

        # Legend
        lx, ly = c7_x + 20, c7_y + 60
        for z, c in focus_colors.items():
            els.append(svg.Rect(x=lx, y=ly, width=15, height=15, fill=c))
            els.append(svg.Text(x=lx+20, y=ly+12, text=z.capitalize(), font_size=12))
            ly += 25

        return svg.SVG(width=W, height=H, elements=els).as_str()

    return (create_dashboard,)


@app.cell
def _(
    create_dashboard,
    knn_dist_slider,
    knn_num_slider,
    mo,
    mode_dropdown,
    show_others,
):
    svg_str = create_dashboard(mode_dropdown.value, show_others.value)

    _JS = """
    <style>
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; position: relative; }
    #tooltip {
        position: absolute;
        background: #ffffff;
        border: 1px solid #d1d8e0;
        padding: 0;
        border-radius: 8px;
        pointer-events: none;
        display: none;
        font-size: 13px;
        box-shadow: 0 8px 24px rgba(0,0,0,0.12);
        z-index: 10000;
        color: #2f3640;
        min-width: 180px;
        overflow: hidden;
    }
    .tt-header {
        background: #f1f2f6;
        padding: 8px 12px;
        font-weight: bold;
        border-bottom: 1px solid #d1d8e0;
        color: #2f3542;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .tt-body { padding: 10px 12px; line-height: 1.6; }
    .tt-row { display: flex; justify-content: space-between; gap: 15px; }
    .tt-label { color: #747d8c; }
    .tt-val { font-weight: 600; color: #2f3542; }
    .tt-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
    /* Fix: apply glow only to the icon parts, not the text */
    .locked-person circle, .locked-person path { 
        filter: drop-shadow(0 0 10px rgba(44, 62, 80, 0.4)); 
        stroke: #2c3e50 !important; 
        stroke-width: 2px !important; 
    }
    </style>
    <div id="tooltip"></div>
    <script>
    (function() {
        let lockedPid = null;
        const tooltip = document.getElementById('tooltip');

        function formatInfo(info) {
            const parts = info.split(' | ');
            if (parts.length < 2) return info;

            let html = `<div class="tt-header">${parts[0]}</div><div class="tt-body">`;
            for (let i = 1; i < parts.length; i++) {
                const sub = parts[i].split(': ');
                if (sub.length === 2) {
                    html += `<div class="tt-row"><span class="tt-label">${sub[0]}</span><span class="tt-val">${sub[1]}</span></div>`;
                } else {
                    html += `<div style="margin-top:4px; font-weight:500;">${parts[i]}</div>`;
                }
            }
            html += '</div>';
            return html;
        }

        function showMember(pid) {
            document.querySelectorAll('.map-dot', '.c6-segment').forEach(d => d.style.display = 'none');
            document.querySelectorAll('.split-info').forEach(s => s.style.display = 'none');
            const mapTotal = document.getElementById('map_total');
            if (mapTotal) mapTotal.style.display = 'none';

            document.querySelectorAll('.visit_' + pid).forEach(d => d.style.display = '');
            const split = document.getElementById('split_' + pid);
            if (split) split.style.display = 'block';

            document.querySelectorAll('.clickable-person').forEach(p => p.classList.remove('locked-person'));
            const pEl = document.querySelector('.pid-' + pid);
            if (pEl) pEl.classList.add('locked-person');
        }

        function resetView() {
            if (lockedPid) return;
            document.querySelectorAll('.map-dot').forEach(d => d.style.display = 'none');
            document.querySelectorAll('.split-info').forEach(s => s.style.display = 'none');
            const mapTotal = document.getElementById('map_total');
            if (mapTotal) mapTotal.style.display = 'block';
            const splitTotal = document.getElementById('split_total');
            if (splitTotal) splitTotal.style.display = 'block';
            document.querySelectorAll('.clickable-person').forEach(p => p.classList.remove('locked-person'));
        }

        function init() {
            const persons = document.querySelectorAll('.clickable-person');
            const segments = document.querySelectorAll('.c7-segment, .c6-segment');

            persons.forEach(p => {
                const pidClass = [...p.classList].find(c => c.startsWith('pid-'));
                if (!pidClass) return;
                const pid = pidClass.replace('pid-', '');

                p.addEventListener('mouseenter', () => { if (!lockedPid) showMember(pid); });
                p.addEventListener('mouseleave', () => resetView());
                p.addEventListener('click', (e) => {
                    e.stopPropagation();
                    if (lockedPid === pid) {
                        lockedPid = null;
                        resetView();
                    } else {
                        lockedPid = pid;
                        showMember(pid);
                    }
                });
            });

            segments.forEach(seg => {
                seg.addEventListener('mouseenter', (e) => {
                    const info = seg.getAttribute('data-info');
                    if (info) {
                        tooltip.style.display = 'block';
                        tooltip.innerHTML = formatInfo(info);
                    }
                });
                seg.addEventListener('mousemove', (e) => {
                    const x = e.pageX + 15;
                    const y = e.pageY + 15;
                    tooltip.style.left = x + 'px';
                    tooltip.style.top = y + 'px';

                    // Adjust if tooltip goes off screen
                    const box = tooltip.getBoundingClientRect();
                    if (x + box.width > window.innerWidth) tooltip.style.left = (e.pageX - box.width - 15) + 'px';
                    if (y + box.height > window.innerHeight) tooltip.style.top = (e.pageY - box.height - 15) + 'px';
                });
                seg.addEventListener('mouseleave', () => {
                    tooltip.style.display = 'none';
                });
            });

            document.addEventListener('click', () => {
                lockedPid = null;
                resetView();
            });
        }

        let timer = setInterval(() => {
            if (document.querySelector('.clickable-person')) {
                clearInterval(timer);
                init();
            }
        }, 100);
    })();
    </script>
    """

    mo.vstack([
        mo.vstack([
            mo.hstack([
                mo.md("Remapper: max distance limit for reference (in km)"),
                knn_dist_slider
            ], align="start", justify="start"),
            mo.hstack([
                mo.md("Remapper: number of nearnest reference locations"),
                knn_num_slider
            ], align="start", justify="start") ,
            mo.hstack([
                mo.md("Comparison mode"),
                mode_dropdown,
            ], align="start", justify="start"),
            mo.hstack([
                mo.md("Show 'Others'"),
                show_others
            ], align="start", justify="start")
        ]),
        mo.iframe(svg_str + _JS, width="1200px", height="920px")
    ])
    return


@app.cell
def _(graph_2_2_data):
    graph_2_2_data
    return


if __name__ == "__main__":
    app.run()
