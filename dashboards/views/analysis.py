"""Analysis: one match from gold, current or historical, on the radar and in charts."""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import data

# 1. Fetch available maps and matches
def get_metadata():
    maps = data.query(f"""
        SELECT DISTINCT r.map_key, r.map_name, r.display_name, r.radar_pos_x, 
                        r.radar_pos_y, r.radar_scale, r.radar_size_px, r.radar_urls, r.has_radar
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

filtered_matches = matches_df[matches_df['map_name'] == selected_map]
match_options = filtered_matches['match_id'].tolist()

if not match_options:
    st.warning("No matches found for the selected map.")
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

rounds_query = f"""
    SELECT DISTINCT round_number, win_team, player_team 
    FROM {data.DIM_ROUND} 
    WHERE match_id = :match_id
    ORDER BY round_number
"""
rounds_df = data.query(rounds_query, match_param)
round_choices = ["All Rounds"] + [f"Round {r}" for r in rounds_df['round_number'].tolist()]
selected_round_str = st.sidebar.selectbox("Round", round_choices)

team_filter = st.sidebar.radio("Team Side", ["Both (All)", "CT", "T"])

vis_mode = st.sidebar.selectbox(
    "Tactical Map Layer",
    [
        "Player Movement Heatmap (KDE)",
        "Kill Locations & Trajectories",
        "Death Locations",
        "Damage Dealt Heatmap"
    ]
)

# --- LOAD RADAR IMAGE ---
radar_urls = map_meta['radar_urls']
radar_img = data.radar_image(selected_map, tuple(radar_urls) if radar_urls is not None else ())

# --- SQL QUERY BUILDING FOR MAP OVERLAYS ---
where_clauses = ["match_id = :match_id"]
overlay_params = dict(match_param)
if selected_round_str != "All Rounds":
    round_no = int(selected_round_str.replace("Round ", ""))
    where_clauses.append("round_number = :round_number")
    overlay_params["round_number"] = round_no
if team_filter != "Both (All)":
    where_clauses.append("player_team = :player_team")
    overlay_params["player_team"] = team_filter

where_str = " AND ".join(where_clauses)

if vis_mode == "Player Movement Heatmap (KDE)":
    query = f"SELECT pos_x, pos_y, player_team FROM {data.FACT_PLAYER_POSITIONS} WHERE {where_str} AND is_alive = true"
elif vis_mode == "Kill Locations & Trajectories":
    query = f"SELECT pos_x, pos_y, player_team, weapon_name, yaw_deg FROM {data.FACT_KILLS} WHERE {where_str}"
elif vis_mode == "Death Locations":
    query = f"SELECT pos_x, pos_y, player_team, weapon_name FROM {data.FACT_DEATHS} WHERE {where_str}"
else:
    query = f"SELECT pos_x, pos_y, player_team, damage FROM {data.FACT_DAMAGE} WHERE {where_str} AND damage_type = 'dealt'"

data_to_plot = data.query(query, overlay_params)

# --- COORDINATE TRANSFORMATION ---
radar_x0 = float(map_meta['radar_pos_x'])
radar_y0 = float(map_meta['radar_pos_y'])
scale = float(map_meta['radar_scale'])
size_px = float(map_meta['radar_size_px'])

if not data_to_plot.empty:
    data_to_plot['pos_x'] = pd.to_numeric(data_to_plot['pos_x'], errors='coerce')
    data_to_plot['pos_y'] = pd.to_numeric(data_to_plot['pos_y'], errors='coerce')
    data_to_plot = data_to_plot.dropna(subset=['pos_x', 'pos_y'])

    data_to_plot['px_x'] = (data_to_plot['pos_x'] - radar_x0) / scale
    data_to_plot['px_y'] = (radar_y0 - data_to_plot['pos_y']) / scale

# --- GLOBAL STYLING ---
plt.style.use('dark_background')

st.title(f"CS2 Match Intel: {map_meta['display_name']}")

# TABS NAVIGATION
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📍 Tactical Radar & Overview", 
    "📈 In-Match Momentum & Economy", 
    "💥 Combat & Damage Metrics",
    "📋 Round Breakdown Table",
    "🏆 Career Form & Matches"
])

# ==========================================
# TAB 1: RADAR & TOP PERFORMANCE
# ==========================================
with tab1:
    col_map, col_stats = st.columns([1.8, 1.2])

    with col_map:
        st.subheader(f"{vis_mode} — {selected_round_str}")
        fig_map, ax = plt.subplots(figsize=(9, 9), dpi=180)
        fig_map.patch.set_facecolor('#0e1117')
        ax.set_facecolor('#0e1117')
        
        if radar_img:
            ax.imshow(radar_img, extent=[0, size_px, size_px, 0], alpha=0.9)

        if data_to_plot.empty:
            st.info("No data points available for the selected filters.")
        else:
            if vis_mode in ["Player Movement Heatmap (KDE)", "Damage Dealt Heatmap"]:
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
            elif vis_mode == "Kill Locations & Trajectories":
                team_palette = {'CT': '#5dade2', 'T': '#e67e22'}
                sns.scatterplot(
                    data=data_to_plot,
                    x='px_x',
                    y='px_y',
                    hue='player_team',
                    palette=team_palette,
                    s=90,
                    edgecolor='white',
                    linewidth=1.2,
                    alpha=0.95,
                    ax=ax
                )
                if 'yaw_deg' in data_to_plot.columns:
                    line_len = 35.0
                    for _, row in data_to_plot.iterrows():
                        yaw_rad = np.radians(row['yaw_deg'])
                        dx = line_len * np.cos(yaw_rad)
                        dy = -line_len * np.sin(yaw_rad)
                        color = '#5dade2' if row['player_team'] == 'CT' else '#e67e22'
                        ax.annotate(
                            '',
                            xy=(row['px_x'] + dx, row['px_y'] + dy),
                            xytext=(row['px_x'], row['px_y']),
                            arrowprops=dict(arrowstyle="->", color=color, lw=1.5, alpha=0.8)
                        )
                ax.legend(title="Killer Team", loc="upper right", framealpha=0.4)

            elif vis_mode == "Death Locations":
                ax.scatter(
                    data_to_plot['px_x'],
                    data_to_plot['px_y'],
                    c='#eb4b4b',
                    marker='X',
                    s=120,
                    edgecolor='black',
                    linewidth=1.0,
                    label='Player Death',
                    zorder=5
                )
                ax.legend(loc="upper right", framealpha=0.4)

        ax.set_xlim(0, size_px)
        ax.set_ylim(size_px, 0)
        ax.axis('off')
        st.pyplot(fig_map)

    with col_stats:
        st.subheader("Match Performance Summary")
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
            fig_pie, ax_pie = plt.subplots(figsize=(5, 5))
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
                t.set_fontsize(11)
            for at in autotexts:
                at.set_color('#ffffff')
                at.set_weight('bold')
                at.set_fontsize(10)
                
            ax_pie.axis('equal')
            st.pyplot(fig_pie)
        else:
            st.info("No recorded kills with weapons for this match.")

# ==========================================
# TAB 2: IN-MATCH MOMENTUM & ECONOMY
# ==========================================
with tab2:
    st.subheader("Round-by-Round Progression & Economic Pressure")
    round_stats_df = data.query(f"""
        SELECT 
            round_number, kills, deaths, damage_dealt, 
            money_start, equip_value, player_won, round_type
        FROM {data.FACT_ROUND_STATS}
        WHERE match_id = :match_id
        ORDER BY round_number ASC
    """, match_param)

    if not round_stats_df.empty:
        round_stats_df['cum_kills'] = round_stats_df['kills'].cumsum()
        round_stats_df['cum_deaths'] = round_stats_df['deaths'].cumsum().replace(0, 1)
        round_stats_df['running_kd'] = round_stats_df['cum_kills'] / round_stats_df['cum_deaths']

        col_mom1, col_mom2 = st.columns(2)
        with col_mom1:
            st.markdown("#### Performance Momentum (Running K/D & Damage)")
            fig_perf, ax1 = plt.subplots(figsize=(7, 4))
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
            st.markdown("#### Economy Dynamics (Cash & Equipment Value)")
            fig_econ, ax_econ = plt.subplots(figsize=(7, 4))
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

# ==========================================
# TAB 3: COMBAT & DAMAGE ANALYSIS
# ==========================================
with tab3:
    st.subheader("Damage Exchange & Round Win Causes")
    col_dmg1, col_dmg2 = st.columns(2)

    with col_dmg1:
        st.markdown("#### Damage Dealt vs. Taken by Weapon")
        dmg_split = data.query(f"""
            SELECT 
                COALESCE(ws.weapon_short_name, REPLACE(d.weapon_name, 'weapon_', '')) AS weapon_label,
                SUM(CASE WHEN d.damage_type = 'dealt' THEN d.damage ELSE 0 END) AS dmg_dealt,
                SUM(CASE WHEN d.damage_type = 'taken' THEN d.damage ELSE 0 END) AS dmg_taken
            FROM {data.FACT_DAMAGE} d
            LEFT JOIN {data.DIM_WEAPON_SPEC} ws ON d.weapon_key = ws.weapon_key
            WHERE d.match_id = :match_id
            GROUP BY 1
            HAVING dmg_dealt > 0 OR dmg_taken > 0
            ORDER BY dmg_dealt DESC
            LIMIT 7
        """, match_param)

        if not dmg_split.empty:
            fig_dmg, ax_dmg = plt.subplots(figsize=(7, 4.2))
            fig_dmg.patch.set_facecolor('#0e1117')
            ax_dmg.set_facecolor('#0e1117')

            x_idx = np.arange(len(dmg_split))
            width = 0.35

            ax_dmg.bar(x_idx - width/2, dmg_split['dmg_dealt'], width, label='Damage Dealt', color='#5dade2', alpha=0.9)
            ax_dmg.bar(x_idx + width/2, dmg_split['dmg_taken'], width, label='Damage Taken', color='#eb4b4b', alpha=0.9)
            ax_dmg.set_xticks(x_idx)
            ax_dmg.set_xticklabels(dmg_split['weapon_label'], rotation=25, color='white')
            ax_dmg.set_ylabel('Total Damage', color='white')
            ax_dmg.tick_params(colors='white')
            ax_dmg.legend(framealpha=0.3)
            st.pyplot(fig_dmg)
        else:
            st.info("No damage events recorded.")

    with col_dmg2:
        st.markdown("#### Round Victory Triggers (Win Reasons)")
        win_reasons = data.query(f"""
            SELECT COALESCE(win_reason, 'unknown') AS win_reason, COUNT(*) as counts
            FROM {data.DIM_ROUND}
            WHERE match_id = :match_id
            GROUP BY 1
            ORDER BY counts DESC
        """, match_param)

        if not win_reasons.empty:
            fig_win, ax_win = plt.subplots(figsize=(7, 4.2))
            fig_win.patch.set_facecolor('#0e1117')
            ax_win.set_facecolor('#0e1117')

            clean_labels = [r.replace('_win_', ' ').replace('_', ' ').title() for r in win_reasons['win_reason']]
            ax_win.barh(clean_labels, win_reasons['counts'], color=['#e67e22', '#5dade2', '#2ecc71'][:len(win_reasons)], alpha=0.85)
            ax_win.set_xlabel('Rounds Count', color='white')
            ax_win.tick_params(colors='white')
            ax_win.invert_yaxis()
            st.pyplot(fig_win)
        else:
            st.info("No round outcome data available.")

# ==========================================
# TAB 4: ROUND BREAKDOWN TABLE
# ==========================================
with tab4:
    st.subheader("Detailed Round-by-Round Log")
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
            s.equip_value AS `Equip Val ($)`
        FROM {data.DIM_ROUND} r
        JOIN {data.FACT_ROUND_STATS} s 
          ON r.round_key = s.round_key
        WHERE r.match_id = :match_id
        ORDER BY r.round_number ASC
    """, match_param)
    
    st.dataframe(round_log, width="stretch")

# ==========================================
# TAB 5: OVERALL FORM & CAREER STATS
# ==========================================
with tab5:
    st.subheader("Multi-Match History & Performance Form")
    all_matches_sum = data.query(f"""
        SELECT 
            match_id, map_name, match_result, kd_ratio, adr, 
            headshot_pct, kills, deaths, rounds_played, started_at
        FROM {data.AGG_MATCH_SUMMARY}
        ORDER BY started_at ASC
    """)

    if not all_matches_sum.empty:
        col_c1, col_c2 = st.columns([1.5, 1])
        
        with col_c1:
            st.markdown("#### K/D Ratio & ADR Trend Across Matches")
            fig_hist, ax_h1 = plt.subplots(figsize=(7, 3.8))
            fig_hist.patch.set_facecolor('#0e1117')
            ax_h1.set_facecolor('#0e1117')

            match_labels = [f"{row['map_name']} ({row['match_result'].upper()})" for _, row in all_matches_sum.iterrows()]
            x_m = np.arange(len(match_labels))

            ax_h1.bar(x_m, all_matches_sum['kd_ratio'], color='#5dade2', alpha=0.8, width=0.4, label='K/D Ratio')
            ax_h1.axhline(1.0, color='red', linestyle='--', alpha=0.6, label='1.0 Parity')
            ax_h1.set_xticks(x_m)
            ax_h1.set_xticklabels(match_labels, color='white')
            ax_h1.set_ylabel('K/D Ratio', color='#5dade2')
            ax_h1.tick_params(colors='white')

            ax_h2 = ax_h1.twinx()
            ax_h2.plot(x_m, all_matches_sum['adr'], color='#f39c12', lw=2.5, marker='o', label='ADR')
            ax_h2.set_ylabel('ADR', color='#f39c12')
            ax_h2.tick_params(colors='white')

            ax_h1.legend(loc='upper left', framealpha=0.3)
            st.pyplot(fig_hist)

        with col_c2:
            st.markdown("#### Career Match History")
            st.dataframe(
                all_matches_sum[['map_name', 'match_result', 'kd_ratio', 'adr', 'kills', 'deaths']],
                width="stretch"
            )

# Every rerun builds new figures. A long-running app server keeps them unless closed.
plt.close("all")