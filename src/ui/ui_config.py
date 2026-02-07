
PAGE_TITLE = "Redshift Optimizer"
LAYOUT = "wide"

LOGO_PATH = "ui/logo.png"
LOGO_WIDTH = 100

AUTO_REFRESH_SECONDS = 60

TABS = [
    "Performance KPIs",
    "Fingerprint Analysis",
    "Optimization",
    "You Are The Optimizer",
    "Query Bubble Game",
    "About Us",
]

ACTIONS_LIMIT = 500
DETAIL_LIMIT = 10
TOP_USAGE_FPS = 5
TOP_WORST_FPS = 25

ABOUT_TITLE = "What is Table Flippers?"
ABOUT_TEXT = """
Table Flippers is a real-time data optimization system designed to bring transparency
to database workloads. We don't just observe; we simulate and analyze to find
efficiency where others see noise.
"""

ABOUT_FEATURES = [
    "Replays real query workloads",
    "Streams them through Kafka",
    "Analyzes them live",
    "Detects redundant patterns using fingerprints",
    "Calculates performance and cost impact",
    "Shows optimization opportunities instantly",
]

TECH_STACK = [
    ("DuckDB", "Stores metrics tables"),
    ("Pandas", "Used in the metrics engine"),
    ("Kafka", "Streams queries in real time"),
    ("Metrics Engine", "Analyzes cost, redundancy, and performance"),
    ("Streamlit UI", "Visualizes live optimization insights"),
]

TEAM = [
    ("Dakshata", "Streaming & Replay Engineer"),
    ("Carolina", "UI / UX Engineer"),
    ("Avanti", "Metrics & Analytics Engineer"),
    ("Hend", "Systems & Integration Lead"),
]

GOOD_TRAITS = [
    "Uses partition pruning",
    "Filters early in WHERE clause",
    "Selects only required columns",
    "Uses proper JOIN keys",
    "Aggregates after filtering",
    "Uses SORTKEY efficiently",
    "Small data scan (<100MB)",
    "Reuses materialized view",
]

BAD_TRAITS = [
    "SELECT * from 10TB table",
    "Cross join without condition",
    "No WHERE clause",
    "Scans entire history table",
    "Functions on JOIN columns",
    "Nested subqueries 5 levels deep",
    "Casting columns in filters",
    "ORDER BY huge dataset",
]