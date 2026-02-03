import streamlit as st
import pandas as pd
import plotly.express as px
# import numpy as np
import datetime
import os
from css import apply_style

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_PATH = os.path.join(BASE_DIR, "data", "consumed", "cleaned_consumed.parquet")


@st.cache_data(ttl=5)
def load_real_data():
    try:
        df = pd.read_parquet(PARQUET_PATH)
    except Exception:
        df = pd.DataFrame()

    # Ensure ALL columns UI expects always exist
    required_cols = [
        "arrival_timestamp",
        "execution_duration_ms",
        "query_type",
        "mbytes_scanned",
        "feature_fingerprint",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors="coerce")
    df["duration_sec"] = df["execution_duration_ms"].fillna(0) / 1000
    #df["query_type"] = (df["query_type"])
    df["mb_scanned"] = df["mbytes_scanned"]
    df["fingerprint"] = df["feature_fingerprint"].astype(str).str.slice(0, 10)
    df["is_redundant"] = False

    return df

def run_redshift_optimizer():
    # Page configuration (Titel)
    st.set_page_config(page_title="Redshift Optimizer", layout="wide")

    # Apply the CSS file
    apply_style()

    # --- INICIALIZE global state ---
    # session for dataframe
    if 'historical_df' not in st.session_state:
        st.session_state.historical_df = pd.DataFrame()

    # Center titel
    head_left, head_center, head_right = st.columns([1, 4, 1])
    
    with head_center:
        st.markdown("<h1 class='main-title'>Redshift Optimization Advisor</h1>", unsafe_allow_html=True)
    
    with head_right:
        st.image("ui/logo.png", width=100)

    # Overall tabs
    tabs = ["Performance KPIs","Fingerprint Analysis","Optimization","You Are The Optimizer","About Us"]
    current_view = st.pills("Dashboard view",tabs, default="Performance KPIs")

    # Refresh part every 60s
    @st.fragment(run_every=60)
    def render_dashboard(view):

        # Load new data
        new_data = load_real_data()
        # Combine data
        if not new_data.empty:
            combined = pd.concat([st.session_state.historical_df, new_data], ignore_index=True)
            # Eliminamos duplicados basados en tiempo y texto para no repetir queries
            st.session_state.historical_df = combined.drop_duplicates(
                subset=['arrival_timestamp', 'query_type']
            ).tail(1000000) 

        df = st.session_state.historical_df

        if df.empty:
            st.info("Waiting for data from Kafka... (Check cleaned_consumed.parquet)")
            empty_col1, empty_col2, empty_col3 = st.columns([1, 1, 1])
            with empty_col2:
                # Puedes usar una URL de un GIF o el path local "ui/loading.gif"
                st.image("https://i.gifer.com/XVo6.gif", width=400) 
                st.divider()
                st.markdown("<p style='text-align: center; color: #7DD3FC;'>Waiting for data from Kafka...</p>", unsafe_allow_html=True)
            return

        df = df.sort_values('timestamp')
        df['is_redundant'] = df.duplicated(subset=['fingerprint'], keep='first') #------

        # Columns for filters vs main title
        col_side, col_main = st.columns([1, 6])

        with col_side:
            st.subheader("Live filters")
            tipos = sorted(df['query_type'].unique().tolist())
            f_type = st.multiselect("Query Type", tipos, default=tipos)

            ## Conditional filter
            #m_choice = None
            #if view == "Optimization":
            #    st.info("Optimization Settings")
            #    m_choice = st.selectbox(
            #        "Metric to Analyze",
            #        ["Potential Saving Money ($)", "Execution Time (Hrs)", "Data Scanned (MB)"]
            #    )
            
            fingerprint = ["All"] + sorted(df['fingerprint'].unique().tolist())
            f_fp = st.selectbox("Fingerprint", fingerprint)

            min_t, max_t = df["timestamp"].min().to_pydatetime(), df["timestamp"].max().to_pydatetime()
            time_range = st.slider("Time range", min_t, max_t, (min_t, max_t), format="HH:mm")

        with col_main:
            
            # Filter time: Get min y max values
            df_filtered = df[
                (df['query_type'].isin(f_type)) & 
                (df['timestamp'] >= time_range[0]) & 
                (df['timestamp'] <= time_range[1])
            ]
            if f_fp != "All":
                df_filtered = df_filtered[df_filtered['fingerprint'] == f_fp]

            # Show time

            st.caption(f"Last update at: {datetime.datetime.now().strftime('%H:%M:%S')}") #---------


            # -- Tab 1: KPIs --
            # Creating boxes/columns (SECC1)
            if view == "Performance KPIs":
                c1, c2, c3, c4 = st.columns(4)

                # Adding data (Part to change)------------------------------
                total_q = len(df_filtered)
                redundant_q = df_filtered['is_redundant'].sum()
                # Calculate savings
                #saved_time = df_filtered[df_filtered['is_redundant'] == True]['duration_sec'].sum() / 3600
                #saved_money = df_filtered[df_filtered['is_redundant'] == True]['mb_scanned'].sum() * 0.00005
                total_mb = (df_filtered['mb_scanned'].sum())/1000000
                last_hour_q = len(new_data)

                # Display KPIs
                c1.metric("Total Queries", f"{total_q}","Last 24Hrs")
                c2.metric("Redundant Queries", f"{redundant_q}", f"{int(redundant_q/total_q*100) if total_q > 0 else 0}%", delta_color="inverse")
                c3.metric("Total data", f"{total_mb:.2f} TB", "Last 24Hrs")
                c4.metric("Last hour queries", f"{last_hour_q}",f"{int(last_hour_q/total_q*100) if total_q > 0 else 0}%")
                #c4.metric("Potential Money Saving", f"${saved_money:.2f}", "Cost Reduction", delta_color="normal")

                st.divider()

                # Creating boxes/columns for 2 graphs
                col_graph, col_pie = st.columns([2, 1])

                # Graph Total Q vs Unique
                with col_graph:
                    st.subheader("Time vs Queries (Total vs Unique)")
                    # Group by time
                    df_time = df_filtered.groupby(df_filtered['timestamp'].dt.hour).agg(
                        total=('is_redundant',lambda x: (x == True).sum()),
                        unique=('is_redundant',lambda x: (x == False).sum()) #---------------------------------- checar
                    ).reset_index()
                    # Create graph
                    fig_line = px.area(df_time, x='timestamp', y=['unique', 'total'], 
                                labels={'value': 'Count', 'timestamp': 'Hour of Day'},
                                color_discrete_sequence=[ "#00CC96",'#ef4444'], template="plotly_dark")
                    # Change display
                    fig_line.update_layout(
                        paper_bgcolor='#0F172A',
                        plot_bgcolor='#0F172A',
                        margin=dict(l=0, r=0, t=20, b=0), 
                        height=230,
                        font=dict(color='#FFFFFF'),
                        legend=dict(font=dict(color='#FFFFFF')),
                        xaxis=dict(tickfont=dict(color='#FFFFFF')),
                        yaxis=dict(tickfont=dict(color='#FFFFFF'))  
                    )
                    st.plotly_chart(fig_line, width="stretch")

                # Piechart Type
                with col_pie:
                    st.subheader("Distribution by Type")
                    # Create Piechart
                    colors = ["#7DD3FC", "#14B8A6", "#8B5CF6", "#F59E0B"]
                    fig_pie = px.pie(df_filtered, names='query_type', hole=0.4, color_discrete_sequence=colors)
                    # Change display
                    fig_pie.update_layout (
                        paper_bgcolor='#0F172A',
                        plot_bgcolor='#0F172A',
                        margin=dict(l=0, r=0, t=20, b=0), 
                        height=220,
                        font=dict(color='#FFFFFF'),
                        legend=dict(font=dict(color='#FFFFFF'))
                        )
                    st.plotly_chart(fig_pie, width="stretch")
                    fig_pie.update_traces(
                    textfont_color='white',
                    marker=dict(line=dict(color='#0B2239', width=2))
                )

            # -- Tab 2: Fingerprints --
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

                # Table top 5 most used queries
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
                        'total_mb': '{:,.2f} MB',   
                        'avg_time': '{:.2f}s'       
                        })
                    st.dataframe(styled_df, width="stretch", hide_index=True)
                    
                # Graph frecuency vs avg execution time
                with col_table:
                    st.subheader("Fingerprint Performance Analysis")
                    # Create Graph
                    df_top25 = df_fp_analysis.sort_values('Time_for_MB', ascending=False).head(25)
                    fig_scatter = px.scatter(df_top25, y='Time_for_MB', x='frequency', 
                                hover_name='fingerprint', color='Time_for_MB', size='Impact_Score',
                                color_continuous_scale='Reds', template="plotly_dark")
                    # Change display
                    fig_scatter.update_layout(
                        paper_bgcolor='#0F172A',
                        plot_bgcolor='#0F172A',
                        margin=dict(l=0, r=0, t=20, b=0), 
                        height=200,
                    )
                    st.plotly_chart(fig_scatter, width="stretch")
                    
                st.divider()
                # Table detailed queries
                st.subheader("Detailed Query Optimization Actions")

                # Create table
                df_detail = df_filtered[['timestamp', 'query_id', 'fingerprint','mb_scanned']].copy()
                #print(type(df_detail["mb_scanned"],type(df_detail["duration_sec"])))
                df_detail["heavt_op"] = df_filtered["mb_scanned"] * df_filtered["duration_sec"]
                df_detail = df_detail.sort_values('heavt_op', ascending=False).head(10)
                df_detail['suggested_action'] = df_detail['fingerprint'] + " - Materialize"
                # Change display
                styled_detail = df_detail[['timestamp', 'query_id', 'fingerprint','suggested_action']].head(10).style.set_properties(**{
                   'background-color': '#1E293B',
                   'color': '#F8FAFC',
                   'border-color': '#475569'
                })
                # Set size
                st.dataframe(
                    styled_detail, 
                    width="stretch", 
                    hide_index=True,
                    height=300 
                )

            # -- Optimization --
            elif view == "Optimization":
                # Create selection section display
                metric_choice = st.selectbox(
                    "Select Metric to Analyze",
                    ["Potential Saving Money ($)", "Execution Time (Hrs)", "Data Scanned (MB)"],
                    key="metric_choice_dropdown" # Key para que no se resetee
                )

                # Calculate totals (Money, time, mb)
                total_money_now = df_filtered['mb_scanned'].sum() * 0.00005
                total_time_now = df_filtered['duration_sec'].sum() / 3600
                total_mb_now = df_filtered['mb_scanned'].sum()

                # Calculate savings
                redundant_mask = df_filtered['is_redundant'] == True
                saving_money = df_filtered[redundant_mask]['mb_scanned'].sum() * 0.00005 
                saving_time = df_filtered[redundant_mask]['duration_sec'].sum() / 3600
                saving_mb = df_filtered[redundant_mask]['mb_scanned'].sum()

                # Select option optimization
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

                # Create data frame
                comparison_data = pd.DataFrame({
                    'Scenario': ['Current (Redundant)', 'After optimization'],
                    'Value': [val_actual, val_projected]
                })

                # Create bar chart
                fig_impact = px.bar(
                    comparison_data, 
                    x='Scenario', 
                    y='Value', 
                    text_auto='.2s',
                    color='Scenario',
                    color_discrete_map={'Current (Redundant)': '#ef4444', 'After optimization': '#00CC96'},
                    template="plotly_dark"
                )

                # Change display
                fig_impact.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=300,
                    font=dict(color='#FFFFFF'),
                    legend=dict(font=dict(color='#FFFFFF')),
                    xaxis=dict(tickfont=dict(color='#FFFFFF')),
                    showlegend=False
                )
                st.plotly_chart(fig_impact, width="stretch")

            elif view == "You Are The Optimizer":

                st.title("You Are the Optimizer")

                st.write(
                    "Below are real query fingerprints from the selected time range.\n"
                    "Pick TWO that you would materialize to save the most warehouse cost."
                )

                # ---- Build metrics from REAL filtered data ----
                df_game = df_filtered.groupby("fingerprint").agg(
                    total_mb=("mb_scanned", "sum"),
                    frequency=("query_id", "count"),
                    avg_time=("duration_sec", "mean")
                ).reset_index()

                df_game["waste_score"] = df_game["total_mb"] * df_game["frequency"]
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

                # ---- Display table with FP labels ----
                st.dataframe(
                    candidates[["fp_label", "frequency", "total_mb", "avg_time"]],
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

            # -- Tab 4: About US --
            elif view == "About Us":
                # What is table flippers
                st.markdown("## 📊 What is Table Flippers?")

                st.write("""
                Table Flippers is a real-time data optimization system designed to bring transparency 
                to database workloads. We don't just observe; we simulate and analyze to find 
                efficiency where others see noise.
                """)
                    
                # Features
                features = [
                    "🔄 **Replays** real query workloads",
                    "🚀 **Streams** them through Kafka",
                    "🕵️ **Analyzes** them live",
                    "🔍 **Detects** redundant patterns using fingerprints",
                    "💰 **Calculates** performance and cost impact",
                    "📈 **Shows** optimization opportunities instantly"
                ]
                for feat in features:
                    st.markdown(f"<div class='arch-step'>{feat}</div>", unsafe_allow_html=True)

                st.divider()

                # Arquitecture
                st.markdown("## Architecture")
                arch_cols = st.columns(5)
                
                tech_stack = [
                    ("DuckDB", "Loads raw query data from parquet"),
                    ("Pandas", "Cleans and prepares the data"),
                    ("Kafka", "Streams queries in real time"),
                    ("Metrics Engine", "Analyzes cost, redundancy, and performance"),
                    ("Streamlit UI", "Visualizes live optimization insights")
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

                # Team presentation
                st.markdown("## 🤝 Meet the Team — Table Flippers")
                team_cols = st.columns(4)
                
                team = [
                    ("Dakshata", "Streaming & Replay Engineer"),
                    ("Carolina", "UI / UX Engineer"),
                    ("Avanti", "Metrics & Analytics Engineer"),
                    ("Hend", "Systems & Integration Lead")
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

# Ejecución
if __name__ == "__main__":
    run_redshift_optimizer()