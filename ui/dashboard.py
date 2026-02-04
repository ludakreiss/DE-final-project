import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import os
import duckdb
from css import apply_style

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "metrics", "metrics.duckdb")


# ---------- DuckDB connection ----------
# @st.cache_resource
# def get_con():
#     # read_only=True is important for stability
#     return duckdb.connect(DB_PATH, read_only=True)


# ---------- Small helpers ----------
def query_df(sql: str, params=None) -> pd.DataFrame:
    with duckdb.connect(DB_PATH, read_only=True) as con:
        if params is None:
            return con.execute(sql).df()
        return con.execute(sql, params).df()



def placeholders(n: int) -> str:
    return ",".join(["?"] * max(n, 1))


def load_filter_domains():
    """
    Get available query_types, fingerprints, and min/max timestamp for the sidebar.
    Use fact table (source of truth).
    """
    domains = query_df("""
        SELECT
            MIN(timestamp) AS min_ts,
            MAX(timestamp) AS max_ts
        FROM fact
    """)
    min_ts = domains["min_ts"].iloc[0]
    max_ts = domains["max_ts"].iloc[0]

    qt = query_df("SELECT DISTINCT query_type FROM fact ORDER BY query_type")
    fps = query_df("SELECT DISTINCT fingerprint FROM fact ORDER BY fingerprint")

    return (
        qt["query_type"].dropna().astype(str).tolist(),
        fps["fingerprint"].dropna().astype(str).tolist(),
        min_ts,
        max_ts,
    )


def filters_active(f_type, all_types, f_fp, time_range, full_range):
    return (
        (set(f_type) != set(all_types)) or
        (f_fp != "All") or
        (time_range != full_range)
    )


def load_fact_filtered(f_type, f_fp, time_range):
    """
    Pull only the rows needed from fact (DuckDB filters in SQL).
    Returns a DataFrame ready for your existing plotting code:
      - includes duration_sec, mb_scanned
    """
    start_ts, end_ts = time_range
    type_list = list(f_type) if f_type else []

    sql = f"""
        SELECT
            timestamp,
            query_id,
            query_type,
            fingerprint,
            mbytes_scanned,
            execution_duration_ms,
            queue_duration_ms,
            compile_duration_ms,
            num_joins,
            num_perm_tables,
            is_redundant,
            charged_seconds,
            cost
        FROM fact
        WHERE timestamp BETWEEN ? AND ?
          AND query_type IN ({placeholders(len(type_list))})
          AND (? = 'All' OR fingerprint = ?)
    """

    params = [start_ts, end_ts, *type_list, f_fp, f_fp]
    df = query_df(sql, params)

    # match your existing downstream code expectations
    df["duration_sec"] = pd.to_numeric(df["execution_duration_ms"], errors="coerce").fillna(0) / 1000.0
    df["mb_scanned"] = pd.to_numeric(df["mbytes_scanned"], errors="coerce").fillna(0)
    df["is_redundant"] = df["is_redundant"].fillna(False).astype(bool)

    return df


def load_actions_filtered(f_type, f_fp, time_range):
    """
    If you want the actions table to respect filters,
    join optimization_actions with fact (to apply query_type filters).
    """
    start_ts, end_ts = time_range
    type_list = list(f_type) if f_type else []

    sql = f"""
        SELECT
            a.timestamp,
            a.query_id,
            a.fingerprint,
            a.suggested_action
        FROM optimization_actions a
        JOIN fact f
          ON f.query_id = a.query_id
        WHERE a.timestamp BETWEEN ? AND ?
          AND f.query_type IN ({placeholders(len(type_list))})
          AND (? = 'All' OR a.fingerprint = ?)
        ORDER BY a.timestamp DESC
        LIMIT 500
    """
    params = [start_ts, end_ts, *type_list, f_fp, f_fp]
    return query_df(sql, params)


# ---------- App ----------
def run_redshift_optimizer():
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = datetime.datetime.now()

    now = datetime.datetime.now()
    if (now - st.session_state.last_refresh).seconds >= 60:
        st.session_state.last_refresh = now
        st.rerun()

    st.set_page_config(page_title="Redshift Optimizer", layout="wide")
    apply_style()

    # header
    head_left, head_center, head_right = st.columns([1, 4, 1])
    with head_center:
        st.markdown("<h1 class='main-title'>Redshift Optimization Advisor</h1>", unsafe_allow_html=True)
    with head_right:
        st.image("ui/logo.png", width=100)

    # Overall tabs
    tabs = ["Performance KPIs","Fingerprint Analysis","Optimization","You Are The Optimizer","Query Bubble Game","About Us"]
    current_view = st.pills("Dashboard view",tabs, default="Performance KPIs")

    @st.fragment(run_every=60)
    def render_dashboard(view):
        # If DB doesn’t exist yet
        st.write("Last Update:", datetime.datetime.fromtimestamp(os.path.getmtime(DB_PATH)))
        if not os.path.exists(DB_PATH):
            st.info("Waiting for metrics.duckdb... (Run metrics.py)")
            empty_col1, empty_col2, empty_col3 = st.columns([1, 1, 1])
            with empty_col2:
                # Puedes usar una URL de un GIF o el path local "ui/loading.gif"
                st.image("https://i.gifer.com/XVo6.gif", width=400) 
                st.divider()
            return

        # domains for filters (from fact)
        all_types, all_fps, min_ts, max_ts = load_filter_domains()
        if min_ts is None or max_ts is None:
            st.info("Waiting for data in fact table...")
            return

        col_side, col_main = st.columns([1, 6])

        with col_side:
            st.subheader("Live filters")

            f_type = st.multiselect("Query Type", all_types, default=all_types)

            fingerprint = ["All"] + all_fps
            f_fp = st.selectbox("Fingerprint", fingerprint)

            full_range = (min_ts.to_pydatetime(), max_ts.to_pydatetime())
            time_range = st.slider("Time range", full_range[0], full_range[1], full_range, format="HH:mm")

        with col_main:
            # st.caption(f"Last update at: {datetime.datetime.now().strftime('%H:%M:%S')}")

            # decide strategy
            use_filtered = filters_active(f_type, all_types, f_fp, time_range, full_range)

            if use_filtered:

                df_filtered = load_fact_filtered(f_type, f_fp, time_range)
            else:
                # no filters: still easiest is to read from fact for the plots,
                # BUT KPIs/charts could read precomputed tables.
                # For minimal changes, we load a window of fact anyway.
                df_filtered = load_fact_filtered(f_type, f_fp, time_range)
                st.write("Filtered rows:", len(df_filtered))
                st.write("Max timestamp:", df_filtered["timestamp"].max())

            if df_filtered.empty:
                st.info("No data for selected filters/time range.")
                return

            # ---------------- TAB 1: KPIs ----------------
            if view == "Performance KPIs":
                c1, c2, c3, c4 = st.columns(4)

                total_q = len(df_filtered)
                redundant_q = int(df_filtered["is_redundant"].sum())
                total_tb = float(df_filtered["mb_scanned"].sum() / 1_000_000)

                # last hour queries (within selected window)
                # If you want literal "last hour" irrespective of slider, use max(timestamp)-1h.
                last_hour_cutoff = df_filtered["timestamp"].max() - pd.Timedelta(hours=1)
                last_hour_q = int((df_filtered["timestamp"] >= last_hour_cutoff).sum())

                c1.metric("Total Queries", f"{total_q}", "Filtered")
                c2.metric("Redundant Queries", f"{redundant_q}",
                          f"{int(redundant_q/total_q*100) if total_q > 0 else 0}%", delta_color="inverse")
                c3.metric("Total data", f"{total_tb:.2f} TB", "Filtered")
                c4.metric("Last hour queries", f"{last_hour_q}",
                          f"{int(last_hour_q/total_q*100) if total_q > 0 else 0}%")

                st.divider()

                col_graph, col_pie = st.columns([2, 1])

                with col_graph:
                    st.subheader("Hour vs Queries (Unique vs Total)")

                    df_time = df_filtered.groupby(df_filtered['timestamp'].dt.hour).agg(
                        total=('is_redundant',lambda x: (x == True).sum()),
                        unique=('is_redundant',lambda x: (x == False).sum())
                    ).reset_index()

                    df_time = df_time.rename(columns={'timestamp': 'hour_of_day'})
                    # NOTE: sometimes the reset_index column name becomes "timestamp" or "arrival_timestamp".
                    # If it didn’t rename, do:
                    # df_time.columns = ['hour_of_day', 'total', 'unique']

                    fig_line = px.area(
                        df_time,
                        x='hour_of_day',
                        y=['unique', 'total'],
                        labels={'value': 'Count', 'hour_of_day': 'Hour of Day'},
                        template="plotly_dark"
                    )


                    fig_line.update_layout(
                        paper_bgcolor="#0F172A",
                        plot_bgcolor="#0F172A",
                        margin=dict(l=0, r=0, t=20, b=0),
                        height=230,
                        font=dict(color="#FFFFFF"),
                        legend=dict(font=dict(color="#FFFFFF")),
                        xaxis=dict(tickfont=dict(color="#FFFFFF")),
                        yaxis=dict(tickfont=dict(color="#FFFFFF")),
                    )
                    st.plotly_chart(fig_line, use_container_width=True)

                with col_pie:
                    st.subheader("Distribution by Type")
                    colors = ["#7DD3FC", "#14B8A6", "#8B5CF6", "#F59E0B"]
                    fig_pie = px.pie(df_filtered, names="query_type", hole=0.4, color_discrete_sequence=colors)
                    fig_pie.update_layout(
                        paper_bgcolor="#0F172A",
                        plot_bgcolor="#0F172A",
                        margin=dict(l=0, r=0, t=20, b=0),
                        height=220,
                        font=dict(color="#FFFFFF"),
                        legend=dict(font=dict(color="#FFFFFF")),
                    )
                    fig_pie.update_traces(
                        textfont_color="white",
                        marker=dict(line=dict(color="#0B2239", width=2)),
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)

            # ---------------- TAB 2: Fingerprints ----------------
            elif view == "Fingerprint Analysis":
                col_table, col_top5 = st.columns([1,1])

                df_fp_analysis = df_filtered.groupby('fingerprint').agg(
                    avg_time=('duration_sec', 'mean'),
                    frequency=('query_id', 'count'),
                    total_mb=('mb_scanned', 'sum')
                ).reset_index()
                
                df_fp_analysis["Time_for_MB"] = df_fp_analysis['avg_time'] / (df_fp_analysis['total_mb'] + 0.00001)
                df_fp_analysis["MB_per_second"] = df_fp_analysis['total_mb'] / (df_fp_analysis['avg_time'])
                df_fp_analysis["Impact_Score"] = df_fp_analysis["Time_for_MB"] * df_fp_analysis["frequency"]

                with col_top5:
                    st.subheader("Top Usage Fingerprints")
                    
                    # Change display
                    df_display = df_fp_analysis.sort_values('frequency', ascending=False).head(5)
                    styled_df = df_display[['fingerprint','avg_time','frequency','total_mb']].style.set_properties(**{
                        'background-color': '#1E293B',
                        'color': '#F8FAFC',           
                        'border-color': '#475569',    
                        'header-color' :'#334155' 
                    }).format({
                        "total_mb": "{:,.2f} MB",
                        "avg_time": "{:.2f}s",
                    })
                    st.dataframe(styled_df, use_container_width=True, hide_index=True)

                with col_table:
                    st.subheader("Fingerprint Performance Analysis")
                    # Create Graph
                    df_top25 = df_fp_analysis.sort_values('Time_for_MB', ascending=False).head(25)
                    fig_scatter = px.scatter(df_top25, y='Time_for_MB', x='frequency', 
                                hover_name='fingerprint', color='Time_for_MB', size='Impact_Score',
                                color_continuous_scale='Reds', template="plotly_dark")
                    # Change display
                    fig_scatter.update_layout(
                        paper_bgcolor="#0F172A",
                        plot_bgcolor="#0F172A",
                        margin=dict(l=0, r=0, t=20, b=0),
                        height=200,
                    )
                    st.plotly_chart(fig_scatter, use_container_width=True)

                st.divider()
                st.subheader("Detailed Query Optimization Actions")

                # List of 25 worst queries
                top_worst_fps = df_top25['fingerprint'].tolist()

                # If you want suggestions to match your metrics.py table:
                actions = load_actions_filtered(f_type, f_fp, time_range)
                if actions.empty:

                    # Only individual queries from the worst fingerprints
                    mask_worst = df_filtered["fingerprint"].isin(top_worst_fps)
                    #slow = df_filtered["execution_duration_ms"].fillna(0) > 5000  # 5 seconds in ms
                    df_detail = (
                        df_filtered.loc[mask_worst, ["timestamp", "query_id", "fingerprint"]]
                            .sort_values("execution_duration_ms", ascending=False)
                            .drop_duplicates(subset=["timestamp", "query_id", "fingerprint"])
                            [["timestamp", "query_id", "fingerprint"]]
                            .copy()
                        )
                    df_detail["suggested_action"] = (
                            df_detail["fingerprint"] + " - Investigate / Optimize"
                        )
                    df_detail = df_detail.head(10).drop_duplicates()
                else:
                    df_detail = actions.head(10).drop_duplicates()

                styled_detail = df_detail.style.set_properties(**{
                    "background-color": "#1E293B",
                    "color": "#F8FAFC",
                    "border-color": "#475569",
                })

                st.dataframe(
                    styled_detail, 
                    width="stretch", 
                    hide_index=True,
                    height=300 
                )

            # ---------------- TAB 3: Optimization ----------------
            elif view == "Optimization":
                metric_choice = st.selectbox(
                    "Select Metric to Analyze",
                    ["Potential Saving Money ($)", "Execution Time (Hrs)", "Data Scanned (MB)"],
                    key="metric_choice_dropdown",
                )

                # Here, your old “money” used mb_scanned * 0.00005; you can keep it or use cost from fact.
                total_money_now = float(df_filtered["cost"].sum())
                total_time_now = float(df_filtered["duration_sec"].sum() / 3600.0)
                total_mb_now = float(df_filtered["mb_scanned"].sum())

                redundant_mask = df_filtered["is_redundant"] == True
                saving_money = float(df_filtered.loc[redundant_mask, "cost"].sum())
                saving_time = float(df_filtered.loc[redundant_mask, "duration_sec"].sum() / 3600.0)
                saving_mb = float(df_filtered.loc[redundant_mask, "mb_scanned"].sum())

                if "Money" in metric_choice:
                    val_actual = total_money_now
                    val_projected = saving_money
                    unit = "$"
                elif "Time" in metric_choice:
                    val_actual = total_time_now
                    val_projected = saving_time
                    unit = "Hrs"
                else:   
                    val_actual = total_mb_now
                    val_projected = saving_mb
                    unit = "MB"

                comparison_data = pd.DataFrame({
                    "Scenario": ["Current (Redundant)", "Unique Queries"],
                    "Value": [val_actual, val_projected],
                })

                fig_impact = px.bar(
                    comparison_data,
                    x="Scenario",
                    y="Value",
                    text_auto=".2s",
                    color="Scenario",
                    color_discrete_map={"Current (Redundant)": "#ef4444", "After optimization": "#00CC96"},
                    template="plotly_dark",
                )

                fig_impact.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    height=300,
                    font=dict(color="#FFFFFF"),
                    legend=dict(font=dict(color="#FFFFFF")),
                    xaxis=dict(tickfont=dict(color="#FFFFFF")),
                    showlegend=False,
                )
                st.plotly_chart(fig_impact, width="stretch")

            elif view == "You Are The Optimizer":

                st.title("You Are the Optimizer")

                st.write(
                    "Below are real query fingerprints from the selected time range.\n"
                    "Pick TWO that you would materialize to save the most warehouse cost."
                )

                # ---- TRUE COST MODEL from parquet columns ----
                df_game = df_filtered.groupby("fingerprint").agg(
                    frequency=("query_id", "count"),
                    total_compile_ms=("compile_duration_ms", "sum"),
                    total_execution_ms=("execution_duration_ms", "sum"),
                    total_queue_ms=("queue_duration_ms", "sum"),
                ).reset_index()

                df_game = df_game[df_game["frequency"] > 2]

                # ---- SESSION STATE INIT ----
                if "game_candidates" not in st.session_state:
                    st.session_state.game_candidates = None

                if "fp_mapping" not in st.session_state:
                    st.session_state.fp_mapping = None

                if "submitted" not in st.session_state:
                    st.session_state.submitted = False

                # ---- Buttons row ----
                col1, col2 = st.columns([6,1])
                with col2:
                    if st.button("Refresh"):
                        st.session_state.game_candidates = None
                        st.session_state.fp_mapping = None
                        st.session_state.submitted = False
                        st.session_state.game_selection = []

                # ---- Generate candidates only if needed ----
                if st.session_state.game_candidates is None:
                    candidates = (
                        df_game.sample(6, replace=False)
                        if len(df_game) >= 6 else df_game
                    ).reset_index(drop=True)

                    # Create FP labels
                    labels = [f"FP-{i+1}" for i in range(len(candidates))]
                    candidates["fp_label"] = labels

                    # Store mapping
                    st.session_state.game_candidates = candidates
                    st.session_state.fp_mapping = dict(
                        zip(labels, candidates["fingerprint"])
                    )
                else:
                    candidates = st.session_state.game_candidates
                # ---- SECRET optimizer formula (hidden from user) ----
                candidates["waste_score"] = (
                    128 * 0.4278 * (
                        candidates["total_compile_ms"] +
                        candidates["total_execution_ms"] +
                        candidates["total_queue_ms"]
                    ) / 1000.0
                )
                # ---- Display table with FP labels ----
                st.dataframe(
                    candidates[[
                        "fp_label",
                        "frequency",
                        "total_compile_ms",
                        "total_execution_ms",
                        "total_queue_ms",
                    ]],
                    use_container_width=True,
                    hide_index=True
                )



                # ---- User selection (labels only) ----
                choices = st.multiselect(
                    "Select TWO fingerprints to materialize:",
                    candidates["fp_label"].tolist(),
                    max_selections=2,
                    key="game_selection"
                )

                # ---- Submit button ----
                if st.button("Submit Answer"):
                    if len(choices) != 2:
                        st.warning("Please select exactly TWO fingerprints.")
                    else:
                        st.session_state.submitted = True
                        st.session_state.user_choices = choices

                # ---- Evaluate ONLY after submit ----
                if st.session_state.submitted:

                    choices = st.session_state.user_choices

                    # Map labels back to real fingerprints
                    real_choices = [
                        st.session_state.fp_mapping[c] for c in choices
                    ]

                    best = candidates.sort_values(
                        "waste_score", ascending=False
                    ).head(2)["fingerprint"].tolist()

                    user_score = candidates[
                        candidates["fingerprint"].isin(real_choices)
                    ]["waste_score"].sum()

                    best_score = candidates[
                        candidates["fingerprint"].isin(best)
                    ]["waste_score"].sum()

                    st.subheader("Result")

                    if set(real_choices) == set(best):
                        st.snow()
                        st.success("Perfect! You think exactly like Table Flippers optimizer 🚀")
                    else:
                        st.error("Not optimal!")

                    st.write("**Your Choice Waste Saved:**", f"{user_score:,.0f}")
                    st.write("**Best Possible Waste Saved:**", f"{best_score:,.0f}")

                    # Reveal answers
                    st.write("What the Optimizer Would Materialize")

                    best_df = candidates[candidates["fingerprint"].isin(best)][
                        ["fp_label", "fingerprint", "waste_score"]
                    ]

                    st.dataframe(best_df, use_container_width=True, hide_index=True)

                    st.write("Your Selection")

                    user_df = candidates[candidates["fingerprint"].isin(real_choices)][
                        ["fp_label", "fingerprint", "waste_score"]
                    ]

                    st.dataframe(user_df, use_container_width=True, hide_index=True)
            elif view == "Query Bubble Game":

                st.title("Build the Perfect Query")
                st.write("Click only the GOOD traits. One bad click = game over.")

                import random

                good_traits = [
                    "Uses partition pruning",
                    "Filters early in WHERE clause",
                    "Selects only required columns",
                    "Uses proper JOIN keys",
                    "Aggregates after filtering",
                    "Uses SORTKEY efficiently",
                    "Small data scan (<100MB)",
                    "Reuses materialized view"
                ]

                bad_traits = [
                    "SELECT * from 10TB table",
                    "Cross join without condition",
                    "No WHERE clause",
                    "Scans entire history table",
                    "Functions on JOIN columns",
                    "Nested subqueries 5 levels deep",
                    "Casting columns in filters",
                    "ORDER BY huge dataset"
                ]

                # -------- SAFE SESSION INIT --------
                for k, v in {
                    "bubble_traits": [],
                    "good_set": set(),
                    "clicked": set(),
                    "game_over": False,
                    "win": False,
                    "message": ""
                }.items():
                    if k not in st.session_state:
                        st.session_state[k] = v

                # -------- Create ONLY 6 bubbles (3 good, 3 bad) --------
                if not st.session_state.bubble_traits:
                    selected_good = random.sample(good_traits, 3)
                    selected_bad = random.sample(bad_traits, 3)

                    traits = selected_good + selected_bad
                    random.shuffle(traits)

                    st.session_state.bubble_traits = traits
                    st.session_state.good_set = set(selected_good)

                # -------- New Game --------
                if st.button("🔄 New Game"):
                    for k in ["bubble_traits", "good_set", "clicked", "game_over", "win", "message"]:
                        st.session_state.pop(k, None)
                    st.rerun()

                traits = st.session_state.bubble_traits

                # -------- Render bubbles (3 columns) --------
                cols = st.columns(3)

                for i, trait in enumerate(traits):

                    # Bubble disappears after click
                    if trait in st.session_state.clicked:
                        continue

                    col = cols[i % 3]

                    with col:
                        if st.button(trait, key=f"bubble_{i}"):

                            st.session_state.clicked.add(trait)

                            # Bad click -> game over
                            if trait not in st.session_state.good_set:
                                st.session_state.game_over = True
                                st.session_state.message = f"💥 Game Over!\n\nBad trait:\n**{trait}**"
                            else:
                                #st.toast("Nice pick! ✅")
                                st.session_state.message = f"✅ Good pick!\n**{trait}**"

                            # Win condition = 3 good clicks
                            if len(st.session_state.clicked & st.session_state.good_set) == 3:
                                st.session_state.win = True
                                st.session_state.message = "🏆 You found all good traits!"

                            st.rerun()

                # -------- Messages --------
                st.divider()

                if st.session_state.message:
                    if st.session_state.game_over:
                        st.error(st.session_state.message)
                    elif st.session_state.win:
                        st.balloons()
                        st.success(st.session_state.message)
            # -- Tab 4: About US --
            elif view == "About Us":
                st.markdown("## 📊 What is Table Flippers?")
                st.write("""
                Table Flippers is a real-time data optimization system designed to bring transparency
                to database workloads. We don't just observe; we simulate and analyze to find
                efficiency where others see noise.
                """)

                features = [
                    "🔄 **Replays** real query workloads",
                    "🚀 **Streams** them through Kafka",
                    "🕵️ **Analyzes** them live",
                    "🔍 **Detects** redundant patterns using fingerprints",
                    "💰 **Calculates** performance and cost impact",
                    "📈 **Shows** optimization opportunities instantly",
                ]
                for feat in features:
                    st.markdown(f"<div class='arch-step'>{feat}</div>", unsafe_allow_html=True)

                st.divider()

                st.markdown("## Architecture")
                arch_cols = st.columns(5)
                tech_stack = [
                    ("DuckDB", "Stores metrics tables"),
                    ("Pandas", "Used in the metrics engine"),
                    ("Kafka", "Streams queries in real time"),
                    ("Metrics Engine", "Analyzes cost, redundancy, and performance"),
                    ("Streamlit UI", "Visualizes live optimization insights"),
                ]
                for i, (tech, desc) in enumerate(tech_stack):
                    with arch_cols[i]:
                        st.markdown(f"""
                            <div class='about-card'>
                                <div style='color: #7DD3FC; font-size: 1.5rem;'>⚙️</div>
                                <strong>{tech}</strong><br>
                                <small style='color: #94A3B8;'>{desc}</small>
                            </div>
                        """, unsafe_allow_html=True)

                st.divider()

                st.markdown("## 🤝 Meet the Team — Table Flippers")
                team_cols = st.columns(4)
                team = [
                    ("Dakshata", "Streaming & Replay Engineer"),
                    ("Carolina", "UI / UX Engineer"),
                    ("Avanti", "Metrics & Analytics Engineer"),
                    ("Hend", "Systems & Integration Lead"),
                ]
                for i, (name, role) in enumerate(team):
                    with team_cols[i]:
                        st.markdown(f"""
                            <div class='about-card' style='text-align: center;'>
                                <div class='team-badge'>{name}</div>
                                <div class='team-role'>{role}</div>
                            </div>
                        """, unsafe_allow_html=True)

                st.markdown("<br><h4 style='text-align: center; color: #7DD3FC;'>Together, we flip database tables, not restaurant tables 🍟</h4>", unsafe_allow_html=True)

    render_dashboard(current_view)


if __name__ == "__main__":
    run_redshift_optimizer()
