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

    # session for tabs
    if 'current_tab_name' not in st.session_state:
        st.session_state.current_tab_name = "Performance KPIs"

    # Refresh part every 60s
    @st.fragment(run_every=60)
    def render_dashboard():

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
            return

        df = df.sort_values('timestamp')
        df['is_redundant'] = df.duplicated(subset=['fingerprint'], keep='first') #------

        # --- FILTROS EN COLUMNAS (Para que funcionen dentro del fragmento) ---
        col_side, col_main = st.columns([1, 5])

        with col_side:
            st.subheader("Live filters")
            tipos = sorted(df['query_type'].unique().tolist())
            f_type = st.multiselect("Query Type", tipos, default=tipos)
            
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

            # --- TITLE ---
            col_title, col_logo = st.columns([6, 1])

            col_title.title("Redshift Optimization Advisor")
            col_title.caption(f"Última actualización: {datetime.datetime.now().strftime('%H:%M:%S')}") #---------
            col_logo.image("ui/logo.png", width=140)

            # -- TABS --
            tabs = ["Performance KPIs","Fingerprint Analysis","Optimization","About Us"]
            tab1, tab2, tab3, tab4 = st.tabs(tabs)

            # Buscamos el índice de la pestaña guardada para que no salte a la primera
            try:
                current_index = tabs.index(st.session_state.current_tab_name)
            except ValueError:
                current_index = 0

            # -- Tab 4: About US --
            with tab4:
                st.session_state.current_tab_name = "About Us"
                st.markdown("### Project Documentation")
                st.write("Aquí irá el texto que pondrás después. Este espacio está diseñado para la descripción general del proyecto y objetivos.")

            # -- Tab 1: KPIs --
            # Creating boxes/columns (SECC1)
            with tab1:
                st.session_state.current_tab_name = "Performance KPIs"
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
                c3.metric("Total MB", f"{total_mb:.2f} TB", "Last 24Hrs")
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
                        total=('query_id', 'count'),
                        unique=('is_redundant', 'count') #---------------------------------- checar
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
            with tab2:
                st.session_state.current_tab_name = "Fingerprint Analysis"
                col_table, col_top5 = st.columns([1,1])

                # Table top 5 most used queries
                with col_top5:
                    st.subheader("Top Usage Fingerprints")
                    # Create table
                    df_fp_analysis = df_filtered.groupby('fingerprint').agg(
                        avg_time=('duration_sec', 'mean'),
                        frequency=('query_id', 'count'),
                        total_mb=('mb_scanned', 'sum')
                    ).reset_index()
                    # Change display
                    df_display = df_fp_analysis.sort_values('total_mb', ascending=False).head(5)
                    styled_df = df_display.style.set_properties(**{
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
                    fig_scatter = px.scatter(df_fp_analysis, x='frequency', y='avg_time', size='total_mb', 
                                    hover_name='fingerprint', color='avg_time', 
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
                df_detail = df_filtered[['timestamp', 'query_id', 'fingerprint']].copy()
                df_detail['suggested_action'] = df_detail['fingerprint'] + " - Materialize"
                # Change display
                styled_detail = df_detail.head(10).style.set_properties(**{
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

                st.dataframe(styled_detail, width="stretch", hide_index=True)

            # -- Optimization --
            with tab3:
                st.session_state.current_tab_name = "Optimization"
                # Create selection section display
                metric_choice = st.radio(
                    "Select Metric to Analyze",
                    ["Potential Saving Money ($)", "Execution Time (Hrs)", "Data Scanned (MB)"],
                    key = metric_choice,
                    horizontal=True
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
                    'Scenario': ['Current (Redundant)', 'With Materialized View'],
                    'Value': [val_actual, val_projected]
                })

                # Create bar chart
                fig_impact = px.bar(
                    comparison_data, 
                    x='Scenario', 
                    y='Value', 
                    text_auto='.2s',
                    color='Scenario',
                    color_discrete_map={'Current (Redundant)': '#ef4444', 'With Materialized View': '#00CC96'},
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
    render_dashboard()

# Ejecución
if __name__ == "__main__":
    run_redshift_optimizer()