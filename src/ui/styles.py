import streamlit as st

def apply_style():
    
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        color: #E6EEF8;
    }

    .stApp {
        background: #0f172a;
        line-height: 2;
    }

    /* Delete space on top of page */
    .block-container {
        padding-top: 4rem !important;
        padding-bottom: 0rem !important;
    }

    /* Reduce space within elements */
    [data-testid="stVerticalBlock"] {
        gap: 0.5rem !important; 
    }
                   
    /* Titels colors */        
    h1, h2, h3 {
        color: #F8FAFC !important;
        font-weight: 800 !important;
        letter-spacing: -0.02em;
        margin-top: 0px !important;
        margin-bottom: 0px !important;
        padding-top: 0px !important;
    }
                
    /* Center title*/
    .main-title {
        text-align: center;
        width: 100%;
        margin-top: -50px; /* Ajuste para subirlo si hay mucho espacio */
        padding-bottom: 10px;
    }
                
    .stRadio [data-testid="stMarkdownContainer"] p { font-size: 20px; font-weight: bold; }
        div[data-testid="stHorizontalBlock"] { background-color: #0F172A; padding: 10px; border-radius: 3px; }

    /*Labels*/
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] h2 {
        color: #7DD3FC !important;
        font-weight: 600;
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
            
    /* secundary texts */
    .st-emotion-cache-16idsys p, 
    .st-emotion-cache-6qob1r,
    [class*="st-"] p {
        color: #7DD3FC !important;
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
        border: 4px solid rgba(125,211,252,0.2);
    }

    /*Remove inner vertical spacing*/
    [data-baseweb="select"] > div {
        padding-top: 2px !important;
        padding-bottom: 1px !important;
    }

    /*Remove hidden extra space*/
    [data-baseweb="select"] div[role="listbox"] {
        margin: 0 !important;
        padding: 0 !important;
    }

    /* Tarjetas de Arquitectura y Equipo */
    .about-card {
        background: #162F47;
        border: 8px solid #223E5C;
        border-radius: 15px;
        padding: 20px;
        margin-bottom: 15px;
        height: 100%;
    }
    
    /* Títulos dentro de las tarjetas de Arquitectura */
    .about-card strong {
        color: #F8FAFC !important; /* Blanco brillante */
        font-size: 1.1rem !important;
        display: block;
        margin-bottom: 5px;
    }

    /* Subtítulos o descripción dentro de las tarjetas */
    .about-card small {
        color: #7DD3FC !important; /* Celeste claro para contraste */
        font-size: 0.9rem !important;
        line-height: 1.2 !important;
        display: block;
    }
                
    .team-badge {
        color: #7DD3FC;
        font-weight: 700;
        font-size: 1rem;
        margin-bottom: 2px;
    }
    .team-role {
        color: #94A3B8;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .arch-step {
        border-left: 3px solid #7DD3FC;
        padding-left: 15px;
        margin-bottom: 1px;
    }
                
    /* Cambiar el color del texto explicativo en About Us */
    [data-testid="stMarkdownContainer"] p {
    color: #E6EEF8 !important; /* Un blanco azulado claro para que resalte */
    line-height: 1.6 !important; /* Espaciado generoso entre líneas */
    }

    /* Si quieres un color específico para la lista de capacidades */
    .arch-step {
        color: #7DD3FC !important; /* El celeste que usas en los KPIs */
        font-size: 1.05rem;
    }
    </style>
    """, unsafe_allow_html=True)