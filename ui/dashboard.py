import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import datetime

def run_redshift_optimizer():
    # Page configuration (Titel)
    # Nota: st.set_page_config debe ser la primera instrucción. 
    # Si se usa dentro de una función, el script principal debe llamarla al inicio.
    st.set_page_config(page_title="Redshift Optimizer", layout="wide")

    # Style (CSS)
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
        
        /* ---- App General Representation ----*/
        /* letter style */
        html, body, [class*="css"] { 
            font-family: 'Inter', monospace; 
            }
        
        /* Background */
        .stApp {
            background-color: #0F172A;
            line-height: 0.1;
        }
                
        /* Wide */
        [data-testid="stMain"]{
            /* No idea, borrar despues*/
            @media (min-width: calc(736px + 1rem)) {
                .st-emotion-cache-zy6yx3 {
                    padding-left: 1.5rem;
                    padding-right: 1.5rem;
                }
            }
            .st-emotion-cache-zy6yx3 {
                width: 100%;
                padding: 2rem 5rem 0rem;
            }   
        }
        
         /* Space to top */
        .st-emotion-cache-zy6yx3 {
            padding-top: 8rem !important;
        }
                
        /* Center main title */
        h1, [data-testid="stMarkdownContainer"] p {
            text-align: center;
            width: 100%;
        }
                
        /* Titels colors */         
        h1, h2, h3 {
            color: #F8FAFC !important;
            font-weight: 800 !important;
            letter-spacing: -0.02em;
        }

        /* Logo circle right */
        .st-emotion-cache-7czcpc > img {
            border-radius: 9rem;
        }
                
        /* Reduce divider space */
        hr {
            margin-top: 0rem !important;
            margin-bottom: 0rem !important;
        }
                    
        /* NO SCROLL config */
        .main {
            overflow: hidden;
            }
        .block-container {
            padding-top: 2rem !important;
            padding-bottom: 0rem !important;
            height: 110vh;
            overflow: hidden;
        }
        
        /* Fit all in one page */
        [data-testid="stVerticalBlock"] { 
            gap: 0.5rem !important; 
                }

        /* Limit graphs high */
        iframe { 
            height: 300px !important;
            max-height: 250px !important;
        } 
        

        /* --- Tabs ---*/
        /* Tabs config*/
        button[data-baseweb="tab"] { 
            font-size: 1.1rem !important; 
            color: #F8FAFC !important; 
        }
            
        /* Tabs visual */
        .stTabs [data-baseweb="tab-list"] {
            gap: 20px;
        }
                
        /* --- Sidebar ---*/      
        /* fixed side bar */
        [data-testid="collapsedControl"] { 
            display: none !important; 
        }
                

        [data-testid="stSidebar"] [data-testid="stImage"] img {
            border-radius: 70%; 
            border: 3px solid #3B82F6; 
            object-fit: cover; 
            width: 130px !important; 
            height: 130px !important;
        }
                
        [data-testid="stSidebar"] { 
            min-width: 250px !important;
            background-color: #0F172A;
            border-right: 1px solid #1E293B; 
        }
                
        /* Multi Select filter visual */
        [data-testid="stMultiSelect"]{
            .st-bb {
                background-color: #DADADA;
                } 
            .st-bq {
                background-color: #3F5AD4;
                }  
        }

        /* Select box filter visual */        
        [data-testid="stSelectbox"]{
            .st-bb {
                background-color: #DADADA;
                }    
        }

        /* --- KPIs Box Design --- */
        /* Metrics style box */         
        [data-testid="stMetric"] {
            background-color: #1E293B;
            border: 5px solid #334155;
            border-radius: 7px;
            padding: 10px 20px;
        }
                
        /* Metrics style numbers */          
        [data-testid="stMetricValue"] {
            font-size: 2.5rem;
            title-color : #F8FAFC;
            font-weight: 600 !important;
        }

        /* Names colors */                 
        [data-testid="stMarkdownContainer"] {
            title-color : #F8FAFC;
            color: #F8FAFC;
        }

        /* --- Table --- */         
        /* Define colors and size */
        [data-testid="stDataFrame"] {
            background-color: #1E293B;
            border: 3px solid #334155;
            border-radius: 10px;
            max-height: 200px !important; 
            overflow-y: auto;
        }

        /* --- Graphs --- */   

        </style>
        """, unsafe_allow_html=True)

    # --- SIMULACIÓN DE DATOS (Lo que vendrá de tus compañeros) ---
    @st.cache_data
    def get_simulated_data():
        np.random.seed(35)
        rows = 200
        data = {
            'timestamp': pd.date_range(start='2024-01-01', periods=rows, freq='15min'),
            'query_id': [f"Q-{i}" for i in range(rows)],
            'fingerprint': [f"FP-{np.random.randint(1, 20)}" for _ in range(rows)],
            'query_type': np.random.choice(['SELECT', 'INSERT', 'UPDATE', 'CTAS'], rows),
            'duration_sec': np.random.uniform(0.5, 30.0, rows),
            'mb_scanned': np.random.uniform(10, 5000, rows),
            'is_redundant': np.random.choice([True, False], rows, p=[0.4, 0.6])
        }
        return pd.DataFrame(data)

    df = get_simulated_data()

    # -- Sidebar Filters --
    # Set filters design and type
    st.sidebar.header("Filters")
    f_type = st.sidebar.multiselect("Query Type", df['query_type'].unique(), default=df['query_type'].unique())
    f_fp = st.sidebar.selectbox("Fingerprint", ["All"] + list(df['fingerprint'].unique()))

    # Filter time: Get min y max values
    min_time = df['timestamp'].min().to_pydatetime()
    max_time = df['timestamp'].max().to_pydatetime()

    # Chane format to minutes
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
    col_title, col_logo = st.columns([4, 1])
    with col_title:
        st.title("Redshift Optimization Advisor")
        st.markdown("Red replay time: 2024-03-03 21:45")
    with col_logo:
        st.markdown('<div class="logo-container">', unsafe_allow_html=True)
        st.image("logo.jpeg")
        st.markdown('</div>', unsafe_allow_html=True)

    # -- TABS --
    tab1, tab2, tab3, tab4 = st.tabs(["About Us","Performance KPIs","Fingerprint Analysis","Optimization"])

    # -- Tab 1: About US --
    with tab1:
        st.markdown("### Project Documentation")
        st.write("Aquí irá el texto que pondrás después. Este espacio está diseñado para la descripción general del proyecto y objetivos.")

    # -- Tab 2: KPIs --
    # Creating boxes/columns (SECC1)
    with tab2:
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
            st.plotly_chart(fig_line, use_container_width=True)

        # Piechart Type
        with col_pie:
            st.subheader("Distribution by Type")
            # Create Piechart
            fig_pie = px.pie(df_filtered, names='query_type', hole=0.4, color_discrete_sequence=px.colors.sequential.RdBu)
            # Change display
            fig_pie.update_layout (
                paper_bgcolor='#0F172A',
                plot_bgcolor='#0F172A',
                margin=dict(l=0, r=0, t=20, b=0), 
                height=220,
                font=dict(color='#FFFFFF'),
                legend=dict(font=dict(color='#FFFFFF'))
                )
            st.plotly_chart(fig_pie, use_container_width=True)
        

    # -- Tab 3: Fingerprints --
    with tab3:
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
            st.dataframe(styled_df, use_container_width=True, hide_index=True)
        
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
            st.plotly_chart(fig_scatter, use_container_width=True)
        
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
            use_container_width=True, 
            hide_index=True,
            height=300 
        )

        st.dataframe(styled_detail, use_container_width=True, hide_index=True)

    # -- Optimization --
    with tab4:
        
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
        st.plotly_chart(fig_impact, use_container_width=True)

# Ejecución
if __name__ == "__main__":
    run_redshift_optimizer()