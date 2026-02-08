import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import os
from styles import apply_style

from ui_config import (PAGE_TITLE, LAYOUT, 
                       LOGO_PATH, LOGO_WIDTH, TABS, AUTO_REFRESH_SECONDS, 
                       ABOUT_TITLE, ABOUT_TEXT,ABOUT_FEATURES, 
                       TECH_STACK, TEAM, GOOD_TRAITS, BAD_TRAITS,
                       TOP_USAGE_FPS, TOP_WORST_FPS, DETAIL_LIMIT)
from data_access import (load_filter_domains, load_fact_filtered, 
                         load_actions_filtered)
from src.optimization import suggest_optimizations


def run_redshift_optimizer():
    # Safe state for updated tables
    if "last_refresh" not in st.session_state:
        st.session_state.last_refresh = datetime.datetime.now()

    # Verify current table was updated in the last minute
    now = datetime.datetime.now()
    if (now - st.session_state.last_refresh).seconds >= AUTO_REFRESH_SECONDS:
        st.session_state.last_refresh = now
        st.rerun()

    # Title for page
    st.set_page_config(page_title=PAGE_TITLE, layout=LAYOUT)
    apply_style()

    # Split header into columns to get title in center and logo in right
    head_left, head_center, head_right = st.columns([1, 4, 1])
    with head_center:
        st.markdown("<h1 class='main-title'>Redshift Optimization Advisor</h1>",
                    unsafe_allow_html=True)
    with head_right:
        st.image(LOGO_PATH, width=LOGO_WIDTH)

    current_view = st.pills("Dashboard view", TABS, default="Performance KPIs")


    # ------------- 60 s refresh loop ----------------
    @st.fragment(run_every=AUTO_REFRESH_SECONDS)
    def render_dashboard(view):
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.join(root_dir, "artifacts", "metrics.duckdb")
        # If DB doesn’t exist yet = loading state
        st.write("Last Update:",
                 datetime.datetime.fromtimestamp(os.path.getmtime(db_path)))
        if not os.path.exists(db_path):
            st.info("Waiting for metrics.duckdb... (Run metrics.py)")
            empty_col1, empty_col2, empty_col3 = st.columns([1, 1, 1])
            with empty_col2:
                # Add gift to loading page
                st.image("https://i.gifer.com/XVo6.gif", width=400)
                st.divider()
            return

        # domains for filters (from fact)
        all_q_types, all_fps, min_ts, max_ts = load_filter_domains()
        if min_ts is None or max_ts is None:
            st.info("Waiting for data in fact table...")
            return
        
        # Divide into filters column and Graphs column
        col_side, col_main = st.columns([1, 6])

        # --------- FIlters -----------
        with col_side:
            st.subheader("Live filters")

            # Query type filter
            qt_filtered = st.multiselect("Query Type", all_q_types, default=all_q_types)

            # Finger print filter
            fingerprint = ["All"] + all_fps
            filtered_fp = st.selectbox("Fingerprint", fingerprint)

            # Get time range and time filter
            full_range = (min_ts.to_pydatetime(), max_ts.to_pydatetime())
            time_range = st.slider("Time range", full_range[0], full_range[1],
                                   full_range, format="HH:mm")


        with col_main:
            df_filtered = load_fact_filtered(qt_filtered, filtered_fp, time_range)

            if df_filtered.empty:
                st.info("No data for selected filters/time range.")
                return

            # ---------------- TAB 1: KPIs ----------------
            if view == "Performance KPIs":
                c1, c2, c3, c4 = st.columns(4)

                #Calculate parameters
                total_q = len(df_filtered)
                redundant_q = int(df_filtered["is_redundant"].sum())
                total_tb = float(df_filtered["mb_scanned"].sum() / 1_000_000)

                # last hour queries (within selected window)
                last_hour_cutoff = df_filtered["timestamp"].max() - pd.Timedelta(hours=1)
                last_hour_q = int((df_filtered["timestamp"] >= last_hour_cutoff).sum())

                # Show KPIs metrics in boxes
                c1.metric("Total Queries", f"{total_q}", "Filtered")
                c2.metric("Redundant Queries", f"{redundant_q}",
                          f"{int(redundant_q/total_q*100) if total_q > 0 else 0}%", 
                          delta_color="inverse")
                c3.metric("Total data", f"{total_tb:.2f} TB", "Filtered")
                c4.metric("Last hour queries", f"{last_hour_q}",
                          f"{int(last_hour_q/total_q*100) if total_q > 0 else 0}%")

                st.divider()

                # Divide for time vs count graph and pie chart 
                col_graph, col_pie = st.columns([2, 1])

                with col_graph:
                    st.subheader("Hour vs Queries (Unique vs Total)")
                    
                    # Get unique and redundant queries
                    df_time = df_filtered.groupby(df_filtered['timestamp'].dt.hour).agg(
                        total=('is_redundant',lambda x: (x == True).sum()),
                        unique=('is_redundant',lambda x: (x == False).sum())
                    ).reset_index()

                    df_time = df_time.rename(columns={'timestamp': 'hour_of_day'})
                    
                    # Define layout of graph and plot
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

                    # Define colors and layout for pie chart
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

                # Aggregate raw executions into fingerprints
                df_fp_analysis = df_filtered.groupby('fingerprint').agg(
                    avg_time=('duration_sec', 'mean'),
                    frequency=('query_id', 'count'),
                    total_mb=('mb_scanned', 'sum')
                ).reset_index()

                # Calculate efficiency scores: Impact = (Time/Data) * Frequency
                df_fp_analysis["Time_for_MB"] = df_fp_analysis['avg_time'] / (df_fp_analysis['total_mb'] + 0.00001)
                df_fp_analysis["MB_per_second"] = df_fp_analysis['total_mb'] / (df_fp_analysis['avg_time'])
                df_fp_analysis["Impact_Score"] = df_fp_analysis["Time_for_MB"] * df_fp_analysis["frequency"]

                with col_top5:
                    st.subheader("Top Usage Fingerprints")

                    # Sort and select top 5 most frecuent fingerprints
                    df_display = df_fp_analysis.sort_values('frequency', ascending=False).head(TOP_USAGE_FPS)
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
                    
                    # Sort by top 25 worst index acording to time per MB
                    df_top25 = df_fp_analysis.sort_values('Time_for_MB', ascending=False).head(TOP_WORST_FPS)
                    
                    # Scatter plot for top 25 worst queries
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
                actions = load_actions_filtered(qt_filtered, filtered_fp, time_range)
                if actions.empty:

                    # Only individual queries from the worst fingerprints
                    mask_worst = df_filtered["fingerprint"].isin(top_worst_fps)

                    # Filter and style the worst-performing queries for detailed inspection
                    df_detail = suggest_optimizations(df_filtered).head(DETAIL_LIMIT).drop_duplicates()
                else:
                    df_detail = actions.head(DETAIL_LIMIT).drop_duplicates()

                # Render interactive dataframe with dark-themed styling
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
                # User selection for impact metric (Cost, Time, or Data)
                metric_choice = st.selectbox(
                    "Select Metric to Analyze",
                    ["Potential Saving Money ($)", "Execution Time (Hrs)", "Data Scanned (MB)"],
                    key="metric_choice_dropdown",
                )

                # Get current values of cost, time and data
                total_money_now = float(df_filtered["cost"].sum())
                total_time_now = float(df_filtered["duration_sec"].sum() / 3600.0)
                total_mb_now = float(df_filtered["mb_scanned"].sum())

                # Get values if they are only unique
                redundant_mask = df_filtered["is_redundant"] == True
                saving_money = float(df_filtered.loc[redundant_mask, "cost"].sum())
                saving_time = float(df_filtered.loc[redundant_mask, "duration_sec"].sum() / 3600.0)
                saving_mb = float(df_filtered.loc[redundant_mask, "mb_scanned"].sum())

                # Assigning values according to selected metric
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

                # Plot the comparison bar
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

            # ---------- Tab Game 1: You are the optimizer -----------
            elif view == "You Are The Optimizer":

                st.title("You Are the Optimizer")

                st.write(
                    "Below are real query fingerprints from the selected time range.\n"
                    "Pick TWO that you would materialize to save the most warehouse cost."
                )

                #  Get true cost model from parquet columns
                df_game = df_filtered.groupby("fingerprint").agg(
                    frequency=("query_id", "count"),
                    total_compile_ms=("compile_duration_ms", "sum"),
                    total_execution_ms=("execution_duration_ms", "sum"),
                    total_queue_ms=("queue_duration_ms", "sum"),
                ).reset_index()

                df_game = df_game[df_game["frequency"] > 2]

                # Create session instances for options and selections
                if "game_candidates" not in st.session_state:
                    st.session_state.game_candidates = None

                if "fp_mapping" not in st.session_state:
                    st.session_state.fp_mapping = None

                if "submitted" not in st.session_state:
                    st.session_state.submitted = False

                # Buttons rows
                col1, col2 = st.columns([6,1])
                with col2:
                    if st.button("Refresh"):
                        st.session_state.game_candidates = None
                        st.session_state.fp_mapping = None
                        st.session_state.submitted = False
                        st.session_state.game_selection = []

                # Generate candidates only if needed
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
                # Optimizer formula 
                candidates["waste_score"] = (
                    128 * 0.4278 * (
                        candidates["total_compile_ms"] +
                        candidates["total_execution_ms"] +
                        candidates["total_queue_ms"]
                    ) / 1000.0
                )
                # Display table with FP labels
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

                # User selection (labels only)
                choices = st.multiselect(
                    "Select TWO fingerprints to materialize:",
                    candidates["fp_label"].tolist(),
                    max_selections=2,
                    key="game_selection"
                )

                # Submit button
                if st.button("Submit Answer"):
                    if len(choices) != 2:
                        st.warning("Please select exactly TWO fingerprints.")
                    else:
                        st.session_state.submitted = True
                        st.session_state.user_choices = choices

                # Evaluate ONLY after submit
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
            
            # --------- Tab Game 2: Query Bubble Game --------
            elif view == "Query Bubble Game":

                st.title("Build the Perfect Query")
                st.write("Click only the GOOD traits. One bad click = game over.")
                

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

                #  Create ONLY 6 bubbles (3 good, 3 bad)
                if not st.session_state.bubble_traits:
                    selected_good = random.sample(GOOD_TRAITS, 3)
                    selected_bad = random.sample(BAD_TRAITS, 3)

                    traits = selected_good + selected_bad
                    random.shuffle(traits)

                    st.session_state.bubble_traits = traits
                    st.session_state.good_set = set(selected_good)

                # Restart the Game
                if st.button("New Game"):
                    for k in ["bubble_traits", "good_set", "clicked", "game_over", "win", "message"]:
                        st.session_state.pop(k, None)
                    st.rerun()

                traits = st.session_state.bubble_traits

                # Render bubbles (3 columns)
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
                                st.session_state.message = f"Game Over!\n\nBad trait:\n**{trait}**"
                            else:
                                # st.toast("Nice pick!")
                                st.session_state.message = f"Good pick!\n**{trait}**"

                            # Win condition = 3 good clicks
                            if len(st.session_state.clicked & st.session_state.good_set) == 3:
                                st.session_state.win = True
                                st.session_state.message = "You found all good traits!"

                            st.rerun()

                # -------- Messages --------
                st.divider()

                if st.session_state.message:
                    if st.session_state.game_over:
                        st.error(st.session_state.message)
                    elif st.session_state.win:
                        st.balloons()
                        st.success(st.session_state.message)

            # ------ Tab 4: About US --------
            elif view == "About Us":
                st.markdown(f"# {ABOUT_TITLE}")
                st.write(ABOUT_TEXT)

                for feat in ABOUT_FEATURES:
                    st.markdown(f"<div class='arch-step'>{feat}</div>", unsafe_allow_html=True)

                st.divider()

                st.markdown("## Architecture")
                arch_cols = st.columns(5)
                for i, (tech, desc) in enumerate(TECH_STACK):
                    with arch_cols[i]:
                        st.markdown(f"""
                            <div class='about-card'>
                                <div style='color: #7DD3FC; font-size: 1.5rem;'>⚙️ </div>
                                <strong>{tech}</strong><br>
                                <small style='color: #94A3B8;'>{desc}</small>
                            </div>
                        """, unsafe_allow_html=True)

                st.divider()

                st.markdown("## Meet the Team — Table Flippers")
                team_cols = st.columns(4)
        
                for i, (name, role) in enumerate(TEAM):
                    with team_cols[i]:
                        st.markdown(f"""
                            <div class='about-card' style='text-align: center;'>
                                <div class='team-badge'>{name}</div>
                                <div class='team-role'>{role}</div>
                            </div>
                        """, unsafe_allow_html=True)

                st.markdown(
                    "<br><h4 style='text-align: center; color: #7DD3FC;'>Together, we flip database tables, not restaurant tables 🍟</h4>",
                    unsafe_allow_html=True)

    # Execute function to refresh dashboard and keep current tab
    render_dashboard(current_view)


# --------- Main --------
if __name__ == "__main__":
    run_redshift_optimizer()
