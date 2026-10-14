"""Analysis: one match from gold, current or historical, on the radar and in charts."""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import data

# --- 1. METADATA LOADING ---
def get_metadata():
    maps = data.query(f"""
        SELECT DISTINCT r.map_key, r.map_name, r.display_name, r.radar_pos_x, 
                        r.radar_pos_y, r.radar_scale, r.radar_size_px, r.radar_urls, 
                        r.thumb_urls, r.has_radar
        FROM {data.DIM_MAP_REFERENCE} r
        WHERE r.has_radar = true
    """)
    
    matches = data.query(f"""
        SELECT m.match_id, m.map_name, m.started_at, m.match_result, m.rounds_played
        FROM {data.DIM_MATCH} m
        ORDER BY m.started_at DESC
    """)
    return maps, matches

maps_df, matches_df = get_metadata()

if maps_df.empty:
    st.warning("No map reference data in gold. Run RUN BATCH on the home page first.")
    st.stop()
if matches_df.empty:
    st.warning("No matches in gold yet. Run RUN ANALYSIS on the home page after a match.")
    st.stop()

# The home page hands over a match to open. Otherwise start on the newest match's map.
focus_match = st.session_state.pop("focus_match", None)
focus_rows = matches_df[matches_df['match_id'] == focus_match]
if not focus_rows.empty:
    st.session_state["map_select"] = focus_rows['map_name'].iloc[0]
    st.session_state["match_select"] = focus_match
elif "map_select" not in st.session_state:
    st.session_state["map_select"] = matches_df['map_name'].iloc[0]

# --- SIDEBAR: FILTERS ---
st.sidebar.title("Telemetry Filters")

map_names = maps_df['map_name'].tolist()
if st.session_state.get("map_select") not in map_names:
    st.session_state.pop("map_select", None)
selected_map = st.sidebar.selectbox(
    "Map", 
    map_names, 
    format_func=lambda x: maps_df.loc[maps_df['map_name'] == x, 'display_name'].values[0] or x,
    key="map_select",
)
map_meta = maps_df[maps_df['map_name'] == selected_map].iloc[0]

# Cascade filter: Match depends directly on selected map
filtered_matches = matches_df[matches_df['map_name'] == selected_map]
match_options = filtered_matches['match_id'].tolist()

if not match_options:
    st.warning("No matches recorded for this map.")
    st.stop()

if st.session_state.get("match_select") not in match_options:
    st.session_state.pop("match_select", None)
selected_match = st.sidebar.selectbox(
    "Match ID", 
    match_options, 
    format_func=lambda x: f"{x[:8]}... ({str(filtered_matches.loc[filtered_matches['match_id'] == x, 'match_result'].values[0]).upper()})",
    key="match_select",
)
match_param = {"match_id": selected_match}

# Available rounds for the chosen match
rounds_query = f"""
    SELECT DISTINCT round_number, win_team, player_team 
    FROM {data.DIM_ROUND} 
    WHERE match_id = :match_id
    ORDER BY round_number
"""
rounds_df = data.query(rounds_query, match_param)
round_choices = ["All Rounds"] + [f"Round {r}" for r in rounds_df['round_number'].tolist()]
selected_round_str = st.sidebar.selectbox("Round Filter", round_choices)
round_no = None if selected_round_str == "All Rounds" else int(selected_round_str.replace("Round ", ""))

team_filter = st.sidebar.radio("Team Perspective", ["Both (All)", "CT", "T"])

vis_mode = st.sidebar.selectbox(
    "Tactical Map Overlay",
    [
        "Kills & Deaths Combined",
        "Player Movement Route (Path)",
        "Player Movement Heatmap (KDE)",
        "Damage Dealt Heatmap"
    ]
)

# --- RADAR & THUMBNAIL IMAGE LOADER ---
radar_urls = map_meta['radar_urls']
thumb_urls = map_meta['thumb_urls']
radar_img = data.radar_image(selected_map, tuple(radar_urls) if radar_urls is not None else ())
thumb_img = data.thumbnail_image(tuple(thumb_urls) if thumb_urls is not None else ())

# --- COORDINATE TRANSFORMATION HELPER ---
radar_x0 = float(map_meta['radar_pos_x'])
radar_y0 = float(map_meta['radar_pos_y'])
scale = float(map_meta['radar_scale'])
size_px = float(map_meta['radar_size_px'])

def transform_coords(df):
    if df.empty:
        return df
    df = df.assign(
        pos_x=pd.to_numeric(df['pos_x'], errors='coerce'),
        pos_y=pd.to_numeric(df['pos_y'], errors='coerce'),
    ).dropna(subset=['pos_x', 'pos_y'])
    return df.assign(
        px_x=(df['pos_x'] - radar_x0) / scale,
        px_y=(radar_y0 - df['pos_y']) / scale,
    )

# --- RADAR DATA QUERIES ---
where_clauses = ["match_id = :match_id"]
overlay_params = dict(match_param)
if round_no is not None:
    where_clauses.append("round_number = :round_number")
    overlay_params["round_number"] = round_no
if team_filter != "Both (All)":
    where_clauses.append("player_team = :player_team")
    overlay_params["player_team"] = team_filter

where_str = " AND ".join(where_clauses)

kills_plot_df = pd.DataFrame()
deaths_plot_df = pd.DataFrame()
data_to_plot = pd.DataFrame()

if vis_mode == "Kills & Deaths Combined":
    q_kills = f"SELECT pos_x, pos_y, player_team, weapon_name, yaw_deg FROM {data.FACT_KILLS} WHERE {where_str}"
    q_deaths = f"SELECT pos_x, pos_y, player_team, weapon_name FROM {data.FACT_DEATHS} WHERE {where_str}"
    kills_plot_df = transform_coords(data.query(q_kills, overlay_params))
    deaths_plot_df = transform_coords(data.query(q_deaths, overlay_params))
elif vis_mode in ["Player Movement Heatmap (KDE)", "Player Movement Route (Path)"]:
    # tick_ts keeps the path in order across rounds; time_in_round_s restarts every round.
    query = f"SELECT pos_x, pos_y, player_team, tick_ts FROM {data.FACT_PLAYER_POSITIONS} WHERE {where_str} AND is_alive = true"
    data_to_plot = transform_coords(data.query(query, overlay_params))
else:  # Damage Dealt Heatmap
    query = f"SELECT pos_x, pos_y, player_team, damage FROM {data.FACT_DAMAGE} WHERE {where_str} AND damage_type = 'dealt'"
    data_to_plot = transform_coords(data.query(query, overlay_params))

plt.style.use('dark_background')

# =========================================================
# DASHBOARD BODY (SINGLE PAGE LAYOUT)
# =========================================================
col_header_text, col_header_thumb = st.columns([4, 1])

with col_header_text:
    match_outcome = str(filtered_matches.loc[filtered_matches['match_id'] == selected_match, 'match_result'].values[0])
    result_color = "#2ecc71" if match_outcome == 'win' else "#e74c3c"
    res_str = match_outcome.upper()
    
    st.markdown(
        f"## {map_meta['display_name'] or selected_map} "
        f"<span style='background-color:{result_color}22; color:{result_color}; border:1px solid {result_color}; "
        f"font-size:0.55em; padding:3px 10px; border-radius:12px; vertical-align:middle;'>{res_str}</span>", 
        unsafe_allow_html=True
    )
    st.caption(f"Match ID: `{selected_match}` | Filtered Round: **{selected_round_str}**")

with col_header_thumb:
    if thumb_img:
        st.image(thumb_img, width="stretch")

# --- SECTION 1: RADAR & MATCH SUMMARY ---
col_map, col_stats = st.columns([1.75, 1.25])

with col_map:
    st.subheader(f"{vis_mode} — {selected_round_str}")
    fig_map, ax = plt.subplots(figsize=(8.5, 8.5), dpi=180)
    fig_map.patch.set_facecolor('#0e1117')
    ax.set_facecolor('#0e1117')
    
    if radar_img:
        ax.imshow(radar_img, extent=[0, size_px, size_px, 0], alpha=0.9)

    if vis_mode == "Kills & Deaths Combined":
        has_data = False
        
        # 1. Kills (circles + aim vector)
        if not kills_plot_df.empty:
            has_data = True
            for team, col, label_name in [('CT', '#5dade2', 'CT Kill'), ('T', '#e67e22', 'T Kill')]:
                sub = kills_plot_df[kills_plot_df['player_team'] == team]
                if not sub.empty:
                    ax.scatter(
                        sub['px_x'], sub['px_y'], 
                        c=col, s=95, marker='o', 
                        edgecolor='white', linewidth=1.2, 
                        label=label_name, zorder=6, alpha=0.95
                    )
            
            if 'yaw_deg' in kills_plot_df.columns:
                line_len = 35.0
                for _, row in kills_plot_df.dropna(subset=['yaw_deg']).iterrows():
                    yaw_rad = np.radians(row['yaw_deg'])
                    dx = line_len * np.cos(yaw_rad)
                    dy = -line_len * np.sin(yaw_rad)
                    arrow_col = '#5dade2' if row['player_team'] == 'CT' else '#e67e22'
                    ax.annotate(
                        '',
                        xy=(row['px_x'] + dx, row['px_y'] + dy),
                        xytext=(row['px_x'], row['px_y']),
                        arrowprops=dict(arrowstyle="->", color=arrow_col, lw=1.5, alpha=0.8),
                        zorder=5
                    )

        # 2. Deaths (X markers with team specific colors)
        if not deaths_plot_df.empty:
            has_data = True
            ct_deaths = deaths_plot_df[deaths_plot_df['player_team'] == 'CT']
            if not ct_deaths.empty:
                ax.scatter(
                    ct_deaths['px_x'], ct_deaths['px_y'],
                    c='#00ffff', marker='X', s=130,
                    edgecolor='#0e1117', linewidth=1.5,
                    label='CT Death', zorder=7, alpha=0.95
                )
            
            t_deaths = deaths_plot_df[deaths_plot_df['player_team'] == 'T']
            if not t_deaths.empty:
                ax.scatter(
                    t_deaths['px_x'], t_deaths['px_y'],
                    c='#ff2a2a', marker='X', s=130,
                    edgecolor='#ffffff', linewidth=1.2,
                    label='T Death', zorder=7, alpha=0.95
                )

        if has_data:
            ax.legend(title="Combat Events", loc="upper right", framealpha=0.6, facecolor='#161b22', edgecolor='#30363d')
        else:
            st.info("No kills or deaths recorded for this selection.")

    elif vis_mode == "Player Movement Route (Path)":
        if data_to_plot.empty:
            st.info("No movement trajectory points available for this selection.")
        else:
            path_data = data_to_plot.sort_values('tick_ts')
            ax.plot(path_data['px_x'], path_data['px_y'], color='#38bdf8', lw=2.2, alpha=0.85, linestyle='-', zorder=4)
            ax.scatter(path_data['px_x'].iloc[0], path_data['px_y'].iloc[0], c='#2ecc71', s=110, marker='o', edgecolors='white', label='Round Start', zorder=5)
            ax.scatter(path_data['px_x'].iloc[-1], path_data['px_y'].iloc[-1], c='#e74c3c', s=120, marker='X', edgecolors='white', label='End / Death', zorder=5)
            ax.legend(loc="upper right", framealpha=0.6, facecolor='#161b22', edgecolor='#30363d')

    else:
        # Heatmap views (KDE or Damage)
        if data_to_plot.empty:
            st.info("No data points available for this selection.")
        else:
            sns.kdeplot(
                data=data_to_plot,
                x='px_x',
                y='px_y',
                fill=True,
                alpha=0.55,
                cmap="turbo" if team_filter == "Both (All)" else ("crest" if team_filter == "CT" else "flare"),
                bw_adjust=0.5,
                thresh=0.05,
                ax=ax
            )

    ax.set_xlim(0, size_px)
    ax.set_ylim(size_px, 0)
    ax.axis('off')
    st.pyplot(fig_map)

with col_stats:
    st.subheader("Match Overview")
    match_sum_df = data.query(f"""
        SELECT kills, deaths, kd_ratio, adr, headshot_pct, rounds_won, rounds_survived
        FROM {data.AGG_MATCH_SUMMARY}
        WHERE match_id = :match_id
    """, match_param)
    
    if not match_sum_df.empty:
        stats = match_sum_df.iloc[0]
        c1, c2 = st.columns(2)
        c1.metric("Kills / Deaths", f"{int(stats['kills'])} / {int(stats['deaths'])}")
        c2.metric("K/D Ratio", f"{stats['kd_ratio']:.2f}")
        c1.metric("ADR", f"{stats['adr']:.1f}")
        hs_val = stats['headshot_pct'] if stats['headshot_pct'] <= 100 else stats['headshot_pct'] / 100
        c2.metric("Headshot %", f"{hs_val:.1f}%")
    
    st.write("---")
    st.subheader("Weapon Distribution (Kills)")
    weapons_used = data.query(f"""
        SELECT 
            COALESCE(ws.weapon_short_name, REPLACE(k.weapon_name, 'weapon_', '')) AS weapon_label,
            COUNT(*) AS kills_count
        FROM {data.FACT_KILLS} k
        LEFT JOIN {data.DIM_WEAPON_SPEC} ws ON k.weapon_key = ws.weapon_key
        WHERE k.match_id = :match_id
        GROUP BY 1
        ORDER BY kills_count DESC
    """, match_param)
    
    if not weapons_used.empty:
        fig_pie, ax_pie = plt.subplots(figsize=(5, 4.5))
        fig_pie.patch.set_facecolor('#0e1117')
        ax_pie.set_facecolor('#0e1117')
        
        cs2_palette = ['#e67e22', '#5dade2', '#eb4b4b', '#f39c12', '#d32ce6', '#8847ff', '#2e86c1']
        colors = cs2_palette[:len(weapons_used)]
        
        wedges, texts, autotexts = ax_pie.pie(
            weapons_used['kills_count'],
            labels=weapons_used['weapon_label'],
            autopct='%1.0f%%',
            pctdistance=0.75,
            startangle=140,
            colors=colors,
            wedgeprops=dict(width=0.45, edgecolor='#0e1117', linewidth=2)
        )
        for t in texts:
            t.set_color('#e2e8f0')
            t.set_fontsize(10)
        for at in autotexts:
            at.set_color('#ffffff')
            at.set_weight('bold')
            at.set_fontsize(9)
            
        ax_pie.axis('equal')
        st.pyplot(fig_pie)
    else:
        st.info("No weapon kills recorded for this match.")

st.write("---")

# --- SECTION 2: MOMENTUM & ECONOMY ---
st.subheader("Match Momentum & Team Economy")
col_mom1, col_mom2 = st.columns(2)

round_stats_df = data.query(f"""
    SELECT 
        round_number, kills, deaths, damage_dealt, 
        money_start, equip_value, player_won, round_type,
        distance_units, time_alive_s
    FROM {data.FACT_ROUND_STATS}
    WHERE match_id = :match_id
    ORDER BY round_number ASC
""", match_param)

if not round_stats_df.empty:
    round_stats_df['cum_kills'] = round_stats_df['kills'].cumsum()
    round_stats_df['cum_deaths'] = round_stats_df['deaths'].cumsum().replace(0, 1)
    round_stats_df['running_kd'] = round_stats_df['cum_kills'] / round_stats_df['cum_deaths']

    with col_mom1:
        st.markdown("##### Running Performance (K/D & Damage Dealt)")
        fig_perf, ax1 = plt.subplots(figsize=(7, 3.8))
        fig_perf.patch.set_facecolor('#0e1117')
        ax1.set_facecolor('#0e1117')
        
        ax1.plot(round_stats_df['round_number'], round_stats_df['running_kd'], color='#5dade2', lw=2.5, marker='o', label='Running K/D')
        ax1.axhline(1.0, color='gray', linestyle='--', alpha=0.6, label='1.0 Parity')
        ax1.set_xlabel('Round Number', color='white')
        ax1.set_ylabel('Running K/D', color='#5dade2')
        ax1.tick_params(colors='white')
        
        ax2 = ax1.twinx()
        ax2.bar(round_stats_df['round_number'], round_stats_df['damage_dealt'], color='#e67e22', alpha=0.3, label='Damage Dealt')
        ax2.set_ylabel('Round Damage', color='#e67e22')
        ax2.tick_params(colors='white')
        ax1.legend(loc='upper left', framealpha=0.3)
        st.pyplot(fig_perf)

    with col_mom2:
        st.markdown("##### Economic Flow (Starting Cash & Equipment Value)")
        fig_econ, ax_econ = plt.subplots(figsize=(7, 3.8))
        fig_econ.patch.set_facecolor('#0e1117')
        ax_econ.set_facecolor('#0e1117')
        
        ax_econ.plot(round_stats_df['round_number'], round_stats_df['equip_value'], color='#2ecc71', lw=2.2, marker='s', label='Equip Value ($)')
        ax_econ.plot(round_stats_df['round_number'], round_stats_df['money_start'], color='#f1c40f', lw=1.8, linestyle=':', marker='^', label='Start Cash ($)')
        
        for _, r in round_stats_df.iterrows():
            color = '#2ecc71' if r['player_won'] else '#e74c3c'
            ax_econ.axvspan(r['round_number'] - 0.4, r['round_number'] + 0.4, color=color, alpha=0.1)

        ax_econ.set_xlabel('Round Number', color='white')
        ax_econ.set_ylabel('USD ($)', color='white')
        ax_econ.tick_params(colors='white')
        ax_econ.legend(loc='upper left', framealpha=0.3)
        st.pyplot(fig_econ)
else:
    st.info("No round statistics available for this match.")

st.write("---")

# --- SECTION 3: COMBAT & ROUND PACING ---
st.subheader("Combat Dynamics & Round Pacing")
col_cb1, col_cb2 = st.columns(2)

with col_cb1:
    st.markdown("##### Kill Engagement Timeline (Time in Round)")
    kills_timing_df = data.query(f"""
        SELECT round_number, time_in_round_s, player_team, weapon_name
        FROM {data.FACT_KILLS}
        WHERE match_id = :match_id AND time_in_round_s IS NOT NULL
        ORDER BY time_in_round_s ASC
    """, match_param)

    if not kills_timing_df.empty:
        fig_timing, ax_t = plt.subplots(figsize=(7, 3.8))
        fig_timing.patch.set_facecolor('#0e1117')
        ax_t.set_facecolor('#0e1117')
        
        sns.histplot(
            data=kills_timing_df,
            x='time_in_round_s',
            hue='player_team',
            palette={'CT': '#5dade2', 'T': '#e67e22'},
            bins=12,
            kde=True,
            ax=ax_t
        )
        ax_t.set_xlabel('Seconds Into Round', color='white')
        ax_t.set_ylabel('Eliminations Count', color='white')
        ax_t.tick_params(colors='white')
        st.pyplot(fig_timing)
    else:
        st.info("No kill timestamps recorded for this match.")

with col_cb2:
    st.markdown("##### Player Mobility & Survival Duration per Round")
    if not round_stats_df.empty:
        fig_mob, ax_m1 = plt.subplots(figsize=(7, 3.8))
        fig_mob.patch.set_facecolor('#0e1117')
        ax_m1.set_facecolor('#0e1117')

        ax_m1.plot(round_stats_df['round_number'], round_stats_df['time_alive_s'], color='#9b59b6', lw=2.2, marker='d', label='Time Alive (s)')
        ax_m1.set_xlabel('Round Number', color='white')
        ax_m1.set_ylabel('Time Alive (s)', color='#9b59b6')
        ax_m1.tick_params(colors='white')

        ax_m2 = ax_m1.twinx()
        ax_m2.plot(round_stats_df['round_number'], round_stats_df['distance_units'], color='#3498db', lw=1.8, linestyle='--', marker='x', label='Distance Units')
        ax_m2.set_ylabel('Distance Traveled', color='#3498db')
        ax_m2.tick_params(colors='white')

        ax_m1.legend(loc='upper left', framealpha=0.3)
        st.pyplot(fig_mob)
    else:
        st.info("No round statistics available for this match.")

st.write("---")

# --- SECTION 4: DYNAMIC ROUND BREAKDOWN TABLE ---
st.subheader("Round Breakdown Table")

table_where = ["r.match_id = :match_id"]
table_params = dict(match_param)
if round_no is not None:
    table_where.append("r.round_number = :round_number")
    table_params["round_number"] = round_no

table_where_clause = " AND ".join(table_where)

round_log = data.query(f"""
    SELECT 
        r.round_number AS `Round`,
        r.player_team AS `Side`,
        r.round_type AS `Buy Type`,
        r.win_team AS `Winner`,
        CASE WHEN r.player_won THEN 'WIN' ELSE 'LOSS' END AS `Result`,
        s.kills AS `Kills`,
        s.headshot_kills AS `HS Kills`,
        s.deaths AS `Deaths`,
        s.damage_dealt AS `DMG Dealt`,
        s.damage_taken AS `DMG Taken`,
        s.money_start AS `Start Cash ($)`,
        s.equip_value AS `Equip Val ($)`,
        ROUND(s.time_alive_s, 1) AS `Time Alive (s)`
    FROM {data.DIM_ROUND} r
    JOIN {data.FACT_ROUND_STATS} s 
      ON r.round_key = s.round_key
    WHERE {table_where_clause}
    ORDER BY r.round_number ASC
""", table_params)

st.dataframe(round_log, width="stretch")

# Every rerun builds new figures. A long-running app server keeps them unless closed.
plt.close("all")
