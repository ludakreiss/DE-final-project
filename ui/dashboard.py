import streamlit as st
import pandas as pd
import plotly.express as px
# import numpy as np
import datetime
import os

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
        "query_label",
        "mbytes_scanned",
        "query_text",
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["timestamp"] = pd.to_datetime(df["arrival_timestamp"], errors="coerce")
    df["duration_sec"] = df["execution_duration_ms"].fillna(0) / 1000
    df["query_type"] = (df["query_text"].astype(str).str.strip().str.split().str[0].str.upper())
    df["mb_scanned"] = df["mbytes_scanned"].fillna(0)
    df["query_id"] = df.index.astype(str)
    df["fingerprint"] = df["query_text"].astype(str).str.slice(0, 80)
    df["is_redundant"] = False

    return df


def run_redshift_optimizer():
    # Page configuration (Titel)
    # Nota: st.set_page_config debe ser la primera instrucción. 
    # Si se usa dentro de una función, el script principal debe llamarla al inicio.
    st.set_page_config(page_title="Redshift Optimizer", layout="wide")

    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        color: #E6EEF8;
    }

    .stApp {
        background: linear-gradient(180deg,#07192a 0%, #0b2239 100%);
    }

    /*Sidebar*/

    [data-testid="stSidebar"] {
        background: #0F2438;
        border-right: 1px solid #1B3A57;
    }

    /*Default sidebar cards*/
    [data-testid="stSidebar"] .element-container {
        background: #162F47;
        border-radius: 12px;
        padding: 14px 16px;
        margin-bottom: 18px;
        border: 1px solid #223E5C;
    }

    /*Labels*/
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] h2 {
        color: #7DD3FC !important;
        font-weight: 600;
    }

    /*Remove black box in multiselect*/
    [data-baseweb="select"] {
        background: transparent !important;
        border: none !important;
    }

    [data-baseweb="select"] > div {
        background: transparent !important;
    }

    /*Pills*/
    [data-baseweb="tag"] {
        background-color: #7DD3FC !important;
        color: #052033 !important;
        font-weight: 700;
        border-radius: 6px !important;
        padding: 3px 8px !important;
        margin: 2px 6px 2px 0 !important;
    }

    /*Pills layout*/
    [data-baseweb="select"] div[role="listbox"] {
        display: flex !important;
        flex-wrap: wrap !important;
        gap: 6px !important;
        margin: 0 !important;
        padding: 0 !important;
    }

    /*Selectbox*/
    [data-baseweb="select"] div[role="button"] {
        background: #0E1F30 !important;
        color: #E6EEF8 !important;
        border-radius: 8px;
    }

    /*Slider*/
    [data-baseweb="slider"] > div {
        background: #1E3A56 !important;
        height: 6px !important;
        border-radius: 8px;
    }

    [data-baseweb="slider"] div[role="slider"] {
        background: #7DD3FC !important;
    }

    .stSlider span {
        color: #7DD3FC !important;
    }

    /*KPIs*/
    [data-testid="stMetric"] {
        background: #102E4A;
        border-radius: 14px;
        padding: 20px;
        border: 1px solid rgba(125,211,252,0.15);
    }

    [data-testid="stMetricValue"] {
        color: #7DD3FC !important;
        font-size: 2.6rem;
        font-weight: 700;
    }

    /*Tables*/
    [data-testid="stDataFrame"] {
        background: #0F2438 !important;
        border-radius: 12px;
        border: 1px solid #223E5C;
    }

    [data-testid="stDataFrame"] th {
        background: #162F47 !important;
        color: #CFE7FF !important;
    }

    [data-testid="stDataFrame"] td {
        background: #0F2438 !important;
        color: #E6EEF8 !important;
    }

    /*Charts*/
    [data-testid="stPlotlyChart"], iframe {
        border-radius: 10px;
        border: 1px solid rgba(125,211,252,0.08);
    }

    /*Logo*/
    [data-testid="stImage"] {
        display: flex;
        justify-content: flex-end;
    }

    [data-testid="stImage"] img {
        border-radius: 100%;
        border: 3px solid rgba(125,211,252,0.2);
    }

    /*Reduce multiselect container height*/
    [data-testid="stSidebar"] .element-container:has([data-baseweb="select"]) {
        padding: 8px 10px !important;
    }

    /*Remove inner vertical spacing*/
    [data-baseweb="select"] > div {
        padding-top: 2px !important;
        padding-bottom: 2px !important;
    }

    /*Remove hidden extra space*/
    [data-baseweb="select"] div[role="listbox"] {
        margin: 0 !important;
        padding: 0 !important;
    }

    </style>
    """, unsafe_allow_html=True)

    # # --- SIMULACIÓN DE DATOS (Lo que vendrá de tus compañeros) ---
    # @st.cache_data
    # def get_simulated_data():
    #     np.random.seed(35)
    #     rows = 200
    #     data = {
    #         'timestamp': pd.date_range(start='2024-01-01', periods=rows, freq='15min'),
    #         'query_id': [f"Q-{i}" for i in range(rows)],
    #         'fingerprint': [f"FP-{np.random.randint(1, 20)}" for _ in range(rows)],
    #         'query_type': np.random.choice(['SELECT', 'INSERT', 'UPDATE', 'CTAS'], rows),
    #         'duration_sec': np.random.uniform(0.5, 30.0, rows),
    #         'mb_scanned': np.random.uniform(10, 5000, rows),
    #         'is_redundant': np.random.choice([True, False], rows, p=[0.4, 0.6])
    #     }
    #     return pd.DataFrame(data)

    df = load_real_data()
    


    # -- Sidebar Filters --
    # Set filters design and type
    st.sidebar.header("Filters")
    f_type = st.sidebar.multiselect("Query Type", df['query_type'].unique(), default=df['query_type'].unique())
    f_fp = st.sidebar.selectbox("Fingerprint", ["All"] + list(df['fingerprint'].unique()))

    # Filter time: Get min y max values

    if df["timestamp"].notna().any():
        min_time = df["timestamp"].min().to_pydatetime()
        max_time = df["timestamp"].max().to_pydatetime()
    else:
        now = datetime.datetime.now()
        min_time = now
        max_time = now + datetime.timedelta(minutes=1)

    time_range = st.sidebar.slider(
        "Time range (minutes)",
        min_value=min_time,
        max_value=max_time,
        value=(min_time, max_time),
        step=datetime.timedelta(minutes=1),
        format="HH:mm"
    )


    # Filter execution
    df_filtered = df[
        (df['query_type'].isin(f_type)) & 
        (df['timestamp'] >= time_range[0]) & 
        (df['timestamp'] <= time_range[1])
    ]

    # Appply filter
    if f_fp != "All":
        df_filtered = df_filtered[df_filtered['fingerprint'] == f_fp]

    # --- TITLE ---
    # col_title, col_logo = st.columns([4, 1])
    # with col_title:
    #     st.title("Redshift Optimization Advisor")
    #     st.markdown("Red replay time: 2024-03-03 21:45")
    # with col_logo:
    #     st.markdown('<div class="logo-container">', unsafe_allow_html=True)
    #     st.image("logo.jpeg")
    #     st.markdown('</div>', unsafe_allow_html=True)
    col_title, col_logo = st.columns([6, 1])

    with col_title:
        st.title("Redshift Optimization Advisor")
        st.caption("Red replay time: 2024-03-03 21:45")

    with col_logo:
        st.image("ui/logo.png", width=140)


    # -- TABS --
    tab1, tab2, tab3, tab4 = st.tabs(["Performance KPIs","Fingerprint Analysis","Optimization","About Us"])

    # -- Tab 4: About US --
    with tab4:
        st.markdown("### Project Documentation")
        st.write("Aquí irá el texto que pondrás después. Este espacio está diseñado para la descripción general del proyecto y objetivos.")

    # -- Tab 1: KPIs --
    # Creating boxes/columns (SECC1)
    with tab1:
        c1, c2, c3, c4 = st.columns(4)

        # Adding data (Part to change)------------------------------
        total_q = len(df_filtered)
        redundant_q = df_filtered['is_redundant'].sum()
        # Calculate savings (Simulando: cada MB escaneado cuesta $0.00001 y cada seg de redundancia es tiempo perdido)
        saved_time = df_filtered[df_filtered['is_redundant'] == True]['duration_sec'].sum() / 3600
        saved_money = df_filtered[df_filtered['is_redundant'] == True]['mb_scanned'].sum() * 0.00005

        # Display KPIs
        c1.metric("Total Queries", f"{total_q}","Last 24Hrs")
        c2.metric("Redundant Queries", f"{redundant_q}", f"{int(redundant_q/total_q*100) if total_q > 0 else 0}%", delta_color="inverse")
        c3.metric("Potential Time Saving", f"{saved_time:.2f} Hrs", "Optimization")
        c4.metric("Potential Money Saving", f"${saved_money:.2f}", "Cost Reduction", delta_color="normal")

        st.divider()

        # Creating boxes/columns for 2 graphs
        col_graph, col_pie = st.columns([2, 1])

        # Graph Total Q vs Unique
        with col_graph:
            st.subheader("Time vs Queries (Total vs Unique)")
            # Group by time
            df_time = df_filtered.groupby(df_filtered['timestamp'].dt.hour).agg(
                total=('query_id', 'count'),
                unique=('fingerprint', 'nunique')
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
        
        # Create selection section display
        metric_choice = st.radio(
            "Select Metric to Analyze",
            ["Potential Saving Money ($)", "Execution Time (Hrs)", "Data Scanned (MB)"],
            horizontal=True
        )

        # 1. Calculamos el Total Actual (Redundantes + No Redundantes)
        total_money_now = df_filtered['mb_scanned'].sum() * 0.00005
        total_time_now = df_filtered['duration_sec'].sum() / 3600
        total_mb_now = df_filtered['mb_scanned'].sum()

        # 2. Calculamos el ahorro potencial (Solo sobre las redundantes)
        # Asumimos que al materializar, el costo de las redundantes baja un 90%
        redundant_mask = df_filtered['is_redundant'] == False
        saving_money = df_filtered[redundant_mask]['mb_scanned'].sum() * 0.00005 
        saving_time = df_filtered[redundant_mask]['duration_sec'].sum() / 3600
        saving_mb = df_filtered[redundant_mask]['mb_scanned'].sum()

        # 3. Selección de métrica para el gráfico
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

        # 4. Crear dataframe para el gráfico
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

# Ejecución
if __name__ == "__main__":
    run_redshift_optimizer()