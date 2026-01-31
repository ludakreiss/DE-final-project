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
        background: linear-gradient(180deg,#07192a 0%, #0b2239 100%);
        line-height: 0.1;
    }
                
    /* Titels colors */        
    h1, h2, h3 {
        color: #F8FAFC !important;
        font-weight: 800 !important;
        letter-spacing: -0.02em;
    }

    /* --- Sidebar ---*/
    /* fixed side bar */
    [data-testid="stSidebarCollapsedControl"], 
    [data-testid="collapsedControl"],
        button[kind="headerNoPadding"] {
            display: none !important;
    }
                
    section[data-testid="stSidebar"] {
        min-width: 280px !important;
        max-width: 280px !important;
        width: 280px !important;
        transform: none !important;
        transition: none !important;
    }
                
    [data-testid="stSidebar"] {
        background: #0F2438;
        border-right: 1px solid #1B3A57;
        min-width: 250px !important;
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
                
    [data-testid="stSidebar"] [data-baseweb="select"] [aria-selected="true"],
    [data-testid="stSidebar"] [data-baseweb="select"] div[role="button"] {
        color: #FF5733 !important; /* He puesto Naranja para que confirmes que funciona, luego cámbialo */
        font-weight: 700 !important;
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