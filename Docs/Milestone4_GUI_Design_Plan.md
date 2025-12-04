# Milestone 4: GUI Application Design Plan
## YouTube Network Analyzer - End-to-End Prototype

**Course:** CPTS 415 - Big Data  
**Team Members:** Ross Kugler, Huy (Harry) Ky, Ben Bordon  
**Date:** November 18, 2025  
**Milestone:** 4 - GUI Prototype Development

---

## 📋 Executive Summary

This document outlines the design plan for the YouTube Analyzer GUI application, which integrates MongoDB databases with Apache Spark analytics to provide an interactive, web-based interface for exploring YouTube video network data. The application will implement all core analytics features developed in Milestones 1-3, including network aggregation, search queries, pattern matching, and influence analysis.

---

## 🎯 Project Objectives

### Primary Goals
1. **Develop an end-to-end GUI application** that connects MongoDB, Spark, and a user-friendly interface
2. **Implement interactive analytics** for all project requirements (network stats, queries, influence analysis)
3. **Provide data visualization** for insights and results
4. **Ensure scalability** for large datasets using efficient Spark operations
5. **Deliver a polished prototype** suitable for demonstration and evaluation

### Success Criteria
- ✅ Successful integration with MongoDB Atlas/local database
- ✅ Execution of all Spark algorithms via GUI
- ✅ Interactive visualizations for all analytics features
- ✅ Responsive UI with loading states and error handling
- ✅ Export functionality for analysis results
- ✅ Documentation for setup and usage

---

## 🛠️ Technology Stack

### Frontend Framework
**Selected: Streamlit** (Python web framework)

**Rationale:**
- **Rapid development**: Minimal code for rich UI components
- **Native Python integration**: Works seamlessly with PySpark and Pandas
- **Built-in visualization**: Matplotlib, Plotly, and Altair support
- **Data-centric**: Designed specifically for data science applications
- **Easy deployment**: Can run locally or deploy to cloud
- **Professional appearance**: Modern, clean interface out-of-the-box

**Alternative Considered:**
- PyQt5/Tkinter (rejected due to complexity and deployment challenges)
- Flask/FastAPI + React (rejected due to time constraints)

### Backend Technologies
- **Apache Spark 3.5.x** (PySpark) - Distributed computing engine
- **MongoDB 7.x** - NoSQL document database
- **Spark MongoDB Connector** - Data integration
- **GraphFrames 0.8.4** - Graph analytics

### Visualization Libraries
- **Plotly** - Interactive charts and graphs
- **Matplotlib** - Static plots and histograms
- **NetworkX** - Graph/network visualizations
- **Pandas** - Data manipulation and tables

### Development Environment
- **Python 3.11+**
- **Java 17** (for Spark)
- **Windows 11** (development OS)
- **Git** - Version control

---

## 📐 Application Architecture

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    USER INTERFACE LAYER                      │
│                    (Streamlit Web App)                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │  Home    │ │ Network  │ │  Queries │ │ Influence│      │
│  │Dashboard │ │  Stats   │ │  & Search│ │ Analysis │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  APPLICATION LOGIC LAYER                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │Query Builder │  │Data Processor│  │Cache Manager │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    DATA PROCESSING LAYER                     │
│                      (Apache Spark)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │Degree Stats  │  │  PageRank    │  │Pattern Search│     │
│  │ (algorithms) │  │  Algorithm   │  │   (Motifs)   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      DATA STORAGE LAYER                      │
│                      (MongoDB Atlas)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   videos     │  │    edges     │  │video_snapshots│    │
│  │  collection  │  │  collection  │  │  collection   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└──────────────────────────────────────────────────────────────┘
```

### Data Flow

```
User Input → Streamlit UI → Query Builder → Spark Processing → MongoDB Query
                                                      │
                                                      ▼
Results ← Visualization ← Data Formatting ← Spark DataFrame
```

---

## 🖥️ GUI Design Specification

### Application Layout

#### Navigation Structure
- **Multi-page Streamlit App** with sidebar navigation
- **Persistent sidebar** across all pages
- **Breadcrumb navigation** for user orientation

#### Page Hierarchy
```
📺 YouTube Analyzer (Home)
├── 🏠 Home Dashboard
├── 📊 Network Statistics
│   ├── Degree Distribution
│   ├── Categorized Statistics
│   └── Graph Metrics
├── 🔍 Top-K Queries
│   ├── Top Categories
│   ├── Top Videos by Views
│   └── Top Videos by Rating
├── 🎯 Range Queries
│   ├── Category Filters
│   ├── Duration Filters
│   └── View Count Filters
├── 🕸️ Pattern Search
│   ├── Motif Query Builder
│   └── Pattern Visualization
├── 💡 Influence Analysis
│   ├── PageRank Computation
│   ├── Influential Videos
│   └── Correlation Analysis
└── ⚙️ Settings
    ├── Database Configuration
    ├── Spark Settings
    └── Cache Management
```

---

## 📱 Detailed Page Specifications

### 1. Home Dashboard

**Purpose:** Provide overview statistics and quick access to features

**Components:**
- **Header Section**
  - Application title and logo
  - Connection status indicators (MongoDB, Spark)
  - Last refresh timestamp

- **Metrics Row** (4 columns)
  - Total Videos: `{count:,}` with trend indicator
  - Total Edges: `{count:,}` with trend indicator
  - Unique Categories: `{count}`
  - Date Range: `{start_date} - {end_date}`

- **Quick Analytics Cards**
  - Most Popular Category (with video count)
  - Most Viewed Video (with details)
  - Average Network Degree
  - Latest Crawl Information

- **Action Buttons**
  - 🔄 Refresh Data
  - 📊 Run Full Analysis
  - 📥 Export All Results
  - 📖 View Documentation

**Technical Implementation:**
```python
# Pseudocode structure
def home_page():
    st.title("📺 YouTube Network Analyzer")
    
    # Connection status
    mongo_status = check_mongo_connection()
    spark_status = check_spark_session()
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Videos", get_video_count())
    col2.metric("Total Edges", get_edge_count())
    col3.metric("Categories", get_category_count())
    col4.metric("Date Range", get_date_range())
    
    # Quick insights
    display_top_category()
    display_top_video()
```

---

### 2. Network Statistics Page

**Purpose:** Display degree distribution and categorized statistics from completed algorithms

**Tab 1: Degree Distribution**

**Input Controls:**
- Sample Size slider: `[1000 - 100000]` (default: 50000)
- Refresh button
- Export button (CSV/JSON)

**Visualizations:**
1. **Aggregate Statistics Cards**
   - Average Total Degree: `{value:.2f}`
   - Average In-Degree: `{value:.2f}`
   - Average Out-Degree: `{value:.2f}`
   - Max Degree: `{value}`
   - Min Degree: `{value}`

2. **Degree Distribution Histogram**
   - X-axis: Degree value
   - Y-axis: Number of videos
   - Type: Bar chart with bin size selector
   - Interactive: Hover for counts

3. **Top 10 Videos by Degree**
   - Table columns: Video ID, Total Degree, In-Degree, Out-Degree
   - Sortable by any column
   - Click to view video details

**Tab 2: Categorized Statistics**

**Visualizations:**
1. **Category Frequency**
   - Bar chart: Top 20 categories by video count
   - Interactive: Click to filter

2. **Length Distribution**
   - Pie chart with buckets:
     - 0-2 minutes
     - 2-5 minutes
     - 5-10 minutes
     - 10-20 minutes
     - \>20 minutes

3. **View Count Distribution**
   - Histogram with buckets:
     - 0-1K
     - 1K-10K
     - 10K-100K
     - 100K-1M
     - 1M-10M
     - \>10M

**Data Source:**
- `Degree_distribution_Categorized_Statistics.py` (existing implementation)
- MongoDB collections: `videos`, `edges`, `degree_statistics`

---

### 3. Top-K Queries Page

**Purpose:** Execute and display top-K analytical queries

**Input Section:**
- K Value selector: `[5, 10, 20, 50, 100]` (dropdown or slider)
- Query type selector (radio buttons):
  - 📈 Top K Categories by Video Count
  - 👁️ Top K Videos by Views
  - ⭐ Top K Videos by Rating
  - 🎬 Top K Videos by PageRank (if computed)

**Results Section:**

**For Category Queries:**
- Table: Category Name | Video Count | Percentage
- Bar chart visualization
- Total videos summary

**For Video Queries:**
- Table: Video ID | Title (if available) | Metric Value | Category
- Expandable rows for details:
  - Uploader
  - Length
  - Upload date
  - Additional stats
- Thumbnail preview (if available)

**Action Buttons:**
- 🔄 Refresh Results
- 📥 Export as CSV
- 📊 Visualize Distribution

**Data Source:**
- `algorithms.ipynb` Step 3 implementation
- MongoDB collections: `video_snapshots`, `videos`

---

### 4. Range Queries Page

**Purpose:** Filter videos by multiple criteria with range specifications

**Filter Panel (Left Sidebar):**

1. **Category Filter**
   - Multi-select dropdown
   - "Select All" / "Clear All" buttons
   - Shows count of selected categories

2. **Duration Range**
   - Dual-handle slider: `[0s - 7200s]`
   - Input boxes for precise values
   - Preset buttons: Short (<5m), Medium (5-20m), Long (>20m)

3. **View Count Range**
   - Dual-handle slider (logarithmic scale)
   - Input boxes for precise values
   - Preset buttons: Viral (>1M), Popular (100K-1M), Growing (<100K)

4. **Date Range** (if crawl dates available)
   - Date picker (start and end)
   - Preset buttons: Last Month, Last Year, All Time

5. **Additional Filters** (expandable)
   - Rating range: `[0.0 - 5.0]`
   - Comments range
   - Related videos count

**Apply Filters Button** (prominent, at bottom of filter panel)

**Results Panel (Main Area):**

1. **Summary Bar**
   - "Showing {count} videos matching your criteria"
   - Clear all filters button
   - Download results button

2. **Results Table**
   - Columns: Video ID, Category, Length, Views, Rating, Crawl Date
   - Sortable by any column
   - Pagination: 50 results per page
   - Row highlighting on hover

3. **Distribution Visualizations** (below table)
   - Category distribution (bar chart)
   - View count histogram
   - Duration distribution

**Data Source:**
- `algorithms.ipynb` Step 3 range query implementation
- MongoDB collections: `video_snapshots`, `videos`

---

### 5. Pattern Search (Motifs) Page

**Purpose:** Find subgraph patterns in the video network

**Query Builder Section:**

1. **Pattern Selector**
   - Visual pattern picker:
     - `(a)-[]->(b)` - Simple directed edge
     - `(a)-[]->(b)-[]->(c)` - Chain pattern
     - `(a)-[]->(b)<-[]->(c)` - V-pattern
     - Custom pattern input (advanced)

2. **Node Constraints**
   - Category filter for each node (a, b, c...)
   - View count threshold
   - Upload date range

3. **Sample Configuration**
   - Sample size slider: `[100 - 10000]` edges
   - Timeout setting: `[10 - 300]` seconds

4. **Run Query Button**

**Results Section:**

1. **Pattern Count**
   - "Found {count} matching patterns"
   - Execution time display

2. **Pattern List** (Table)
   - Columns: Node A, Relationship, Node B (and C if applicable)
   - Shows video IDs and categories
   - Expandable rows for details

3. **Sample Visualization**
   - Network graph showing first 5-10 matching patterns
   - Interactive: Click nodes to see details
   - Color-coded by category

4. **Statistics Panel**
   - Most common categories in patterns
   - Average path length
   - Pattern distribution

**Data Source:**
- `algorithms.ipynb` Step 4 motif implementation
- GraphFrames find() function
- MongoDB collections: `videos`, `edges`

---

### 6. Influence Analysis (PageRank) Page

**Purpose:** Compute and analyze video influence using PageRank algorithm

**Configuration Section:**

1. **PageRank Parameters**
   - Reset Probability: Slider `[0.05 - 0.30]` (default: 0.15)
   - Max Iterations: Input `[5 - 50]` (default: 10)
   - Convergence Tolerance: Input `[0.0001 - 0.1]` (default: 0.01)

2. **Graph Sampling** (for performance)
   - Sample size: `[5000 - 100000]` edges
   - Checkbox: "Use full graph" (warning for large datasets)

3. **Run PageRank Button** (large, prominent)
   - Shows estimated time
   - Progress bar during computation

**Results Section:**

**Tab 1: Top Influential Videos**

1. **Top K Table** (K selector: 10/20/50/100)
   - Columns: Rank, Video ID, PageRank Score, Views, Category, In-Degree, Out-Degree
   - Sortable
   - Row expansion for details

2. **PageRank Distribution**
   - Histogram showing score distribution
   - Log scale option

**Tab 2: Correlation Analysis**

1. **PageRank vs Views**
   - Scatter plot with trend line
   - Correlation coefficient display
   - Outlier highlighting

2. **PageRank vs Degree**
   - Scatter plot: PageRank score vs Total Degree
   - Color-coded by category

3. **Category Analysis**
   - Box plot: PageRank distribution by category
   - Table: Average PageRank per category

**Tab 3: Video Insights**

- Cards for top 5 influential videos:
  - Video ID
  - PageRank score (with percentile)
  - View count
  - Category
  - Network position metrics
  - "Why is this influential?" explanation (heuristic-based)

**Export Options:**
- Full PageRank results (CSV)
- Top K list (PDF report)
- Visualizations (PNG)

**Data Source:**
- `algorithms.ipynb` Step 5 PageRank implementation
- GraphFrames pageRank() function
- MongoDB collections: `videos`, `edges`, `video_snapshots`

---

### 7. Settings Page

**Purpose:** Configure application behavior and connections

**MongoDB Configuration:**
- Connection URI input
- Database name input
- Test connection button
- Connection status indicator
- Collection selection (checkboxes for which to load)

**Spark Configuration:**
- Driver memory: `[2g - 8g]`
- Executor memory: `[2g - 8g]`
- Shuffle partitions: `[4 - 32]`
- Master URL (local[\*] or cluster address)
- Restart Spark session button

**Performance Settings:**
- Default sample size for queries
- Enable/disable caching
- Cache timeout (minutes)
- Clear cache button

**Export Settings:**
- Default export format: CSV/JSON/Excel
- Export directory selection
- Include metadata checkbox

**Advanced:**
- Log level selector (INFO/WARN/ERROR)
- View logs button
- Reset to defaults button

---

## 🔧 Technical Implementation Details

### File Structure

```
Youtube_Analyzer/
├── app/
│   ├── main.py                          # Streamlit entry point
│   ├── pages/
│   │   ├── 1_🏠_Home.py                # Home dashboard
│   │   ├── 2_📊_Network_Statistics.py  # Degree & categorized stats
│   │   ├── 3_🔍_TopK_Queries.py        # Top-K query interface
│   │   ├── 4_🎯_Range_Queries.py       # Range query filters
│   │   ├── 5_🕸️_Pattern_Search.py      # Motif search
│   │   ├── 6_💡_Influence_Analysis.py  # PageRank analysis
│   │   └── 7_⚙️_Settings.py            # Configuration
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── spark_connector.py          # Spark session management
│   │   ├── mongo_connector.py          # MongoDB connection
│   │   ├── query_engine.py             # Query execution logic
│   │   ├── visualizations.py           # Chart generation functions
│   │   ├── data_processor.py           # Data transformation utilities
│   │   └── cache_manager.py            # Streamlit caching helpers
│   ├── components/
│   │   ├── __init__.py
│   │   ├── metrics_cards.py            # Reusable metric displays
│   │   ├── data_tables.py              # Enhanced table components
│   │   └── filters.py                  # Reusable filter widgets
│   ├── config/
│   │   ├── __init__.py
│   │   ├── app_config.py               # Application settings
│   │   ├── spark_config.py             # Spark configuration
│   │   └── mongo_config.py             # MongoDB settings
│   └── assets/
│       ├── styles.css                   # Custom CSS
│       ├── logo.png                     # App logo
│       └── README.md                    # App documentation
├── Spark/
│   ├── algorithms.ipynb                 # [EXISTING] Core algorithms
│   ├── Degree_distribution_Categorized_Statistics.py  # [EXISTING]
│   └── spark_algorithms.py              # [NEW] Refactored for GUI integration
├── data/
│   └── raw/                             # Raw data files (if local)
├── Docs/
│   ├── Milestone4_GUI_Design_Plan.md    # This document
│   ├── Milestone4_User_Guide.md         # [TO CREATE] User documentation
│   └── Milestone4_Technical_Docs.md     # [TO CREATE] Technical reference
├── requirements.txt                     # Python dependencies
├── .streamlit/
│   └── config.toml                      # Streamlit configuration
├── README.md                            # Project README
└── .gitignore
```

### Key Python Modules

#### 1. `app/utils/spark_connector.py`

**Purpose:** Manage Spark session lifecycle

**Functions:**
```python
def get_spark_session(config: dict = None) -> SparkSession
def stop_spark_session() -> None
def restart_spark_session(config: dict = None) -> SparkSession
def check_spark_status() -> dict
```

**Features:**
- Singleton pattern for session management
- Automatic MongoDB connector loading
- GraphFrames package integration
- Error handling and logging
- Streamlit caching integration

#### 2. `app/utils/mongo_connector.py`

**Purpose:** MongoDB connection and basic operations

**Functions:**
```python
def get_mongo_client(uri: str = None) -> MongoClient
def test_connection(uri: str) -> bool
def get_collection_stats(db: str, collection: str) -> dict
def get_collections_list(db: str) -> list
def load_collection_to_spark(collection: str) -> DataFrame
```

#### 3. `app/utils/query_engine.py`

**Purpose:** Execute analytics queries using existing algorithms

**Functions:**
```python
def compute_degree_statistics(sample_size: int) -> dict
def run_topk_query(query_type: str, k: int) -> DataFrame
def run_range_query(filters: dict) -> DataFrame
def find_patterns(pattern: str, filters: dict) -> DataFrame
def compute_pagerank(params: dict) -> DataFrame
```

**Integration:**
- Imports from `Spark/algorithms.ipynb` (refactored as .py)
- Wraps Spark operations with error handling
- Returns results as Pandas DataFrames for Streamlit

#### 4. `app/utils/visualizations.py`

**Purpose:** Generate charts and graphs

**Functions:**
```python
def plot_degree_distribution(df: pd.DataFrame) -> plotly.Figure
def plot_category_bar_chart(df: pd.DataFrame) -> plotly.Figure
def plot_scatter_correlation(df: pd.DataFrame, x: str, y: str) -> plotly.Figure
def plot_network_graph(vertices: list, edges: list) -> matplotlib.Figure
def create_metrics_row(metrics: dict) -> None  # Streamlit component
```

#### 5. `app/components/filters.py`

**Purpose:** Reusable filter UI components

**Functions:**
```python
def category_multiselect(categories: list, key: str) -> list
def dual_range_slider(min_val: float, max_val: float, label: str, key: str) -> tuple
def date_range_picker(key: str) -> tuple
def sample_size_selector(key: str) -> int
```

---

## 🎨 UI/UX Design Guidelines

### Design Principles

1. **Data-First Interface**
   - Emphasize data and insights over decorative elements
   - Use white space effectively
   - Clear visual hierarchy

2. **Progressive Disclosure**
   - Show essential options first
   - Advanced settings in expanders
   - Tooltips for complex parameters

3. **Feedback & Responsiveness**
   - Loading indicators for all operations
   - Success/error messages
   - Estimated time for long operations
   - Progress bars where applicable

4. **Consistency**
   - Uniform color scheme across pages
   - Consistent button styles
   - Standard metric card format
   - Predictable navigation

### Color Scheme

**Primary Colors:**
- Primary: `#FF4B4B` (Streamlit red) - Action buttons, highlights
- Secondary: `#0068C9` (Blue) - Links, secondary actions
- Success: `#09AB3B` (Green) - Positive metrics, success states
- Warning: `#FFA421` (Orange) - Warnings, cautions
- Error: `#FF2C2C` (Red) - Errors, critical issues

**Background & Text:**
- Background: `#FFFFFF` (White) / `#0E1117` (Dark mode)
- Text Primary: `#262730` / `#FAFAFA`
- Text Secondary: `#808495` / `#A3A8B8`
- Border: `#E6E9EF` / `#38404E`

**Data Visualization:**
- Use Plotly default color scheme (colorblind-friendly)
- Consistent category colors across visualizations
- Highlight important data points with red/orange

### Typography

- **Headers:** `Sans-serif` (Streamlit default)
- **Body:** `Sans-serif`
- **Code/IDs:** `Monospace`
- **Sizes:**
  - Page title: 32px
  - Section header: 24px
  - Card header: 18px
  - Body text: 14px
  - Caption: 12px

### Spacing

- Page margins: 2rem
- Section spacing: 1.5rem
- Component spacing: 1rem
- Compact spacing: 0.5rem

---

## 🚀 Implementation Roadmap

### Phase 1: Foundation (Week 1 - Days 1-3)

**Day 1: Project Setup**
- [ ] Create `app/` directory structure
- [ ] Set up Streamlit multi-page app skeleton
- [ ] Create `requirements.txt` with all dependencies
- [ ] Configure `.streamlit/config.toml`
- [ ] Set up Git branch for Milestone 4

**Day 2: Core Infrastructure**
- [ ] Implement `spark_connector.py`
- [ ] Implement `mongo_connector.py`
- [ ] Test MongoDB connection with existing database
- [ ] Test Spark session initialization
- [ ] Create configuration files

**Day 3: Home Page & Navigation**
- [ ] Build Home Dashboard with metrics
- [ ] Implement sidebar navigation
- [ ] Create connection status indicators
- [ ] Test page routing

### Phase 2: Core Analytics Features (Week 1 - Days 4-7)

**Day 4: Network Statistics Page**
- [ ] Implement degree distribution display
- [ ] Integrate with existing `Degree_distribution_Categorized_Statistics.py`
- [ ] Create degree histogram visualization
- [ ] Build categorized statistics view

**Day 5: Top-K Queries Page**
- [ ] Implement query selector UI
- [ ] Integrate with `algorithms.ipynb` Step 3
- [ ] Create results table with sorting
- [ ] Add export functionality

**Day 6: Range Queries Page**
- [ ] Build filter panel with all controls
- [ ] Implement filter logic with Spark
- [ ] Create results display
- [ ] Add distribution visualizations

**Day 7: Testing & Refinement**
- [ ] Test all implemented features
- [ ] Fix bugs and edge cases
- [ ] Optimize query performance
- [ ] Improve UI responsiveness

### Phase 3: Advanced Features (Week 2 - Days 8-11)

**Day 8: Pattern Search Page**
- [ ] Build pattern selector UI
- [ ] Implement motif queries with GraphFrames
- [ ] Create pattern list display
- [ ] Add basic network visualization

**Day 9: Influence Analysis Page (Part 1)**
- [ ] Implement PageRank parameter controls
- [ ] Integrate with `algorithms.ipynb` Step 5
- [ ] Create progress indicator for computation
- [ ] Build top influential videos table

**Day 10: Influence Analysis Page (Part 2)**
- [ ] Create correlation analysis visualizations
- [ ] Build PageRank distribution charts
- [ ] Add category analysis section
- [ ] Implement export options

**Day 11: Settings Page**
- [ ] Build MongoDB configuration UI
- [ ] Add Spark settings controls
- [ ] Implement cache management
- [ ] Create export preferences section

### Phase 4: Polish & Documentation (Week 2 - Days 12-14)

**Day 12: Optimization**
- [ ] Implement Streamlit caching strategies
- [ ] Optimize Spark queries for performance
- [ ] Add loading states and spinners
- [ ] Improve error handling

**Day 13: UI/UX Refinement**
- [ ] Apply custom CSS styling
- [ ] Improve responsive layout
- [ ] Add helpful tooltips
- [ ] Enhance data table displays
- [ ] Add export buttons to all pages

**Day 14: Documentation & Testing**
- [ ] Write user guide (Milestone4_User_Guide.md)
- [ ] Create technical documentation
- [ ] Test all features end-to-end
- [ ] Prepare demo scenarios
- [ ] Create demo video/screenshots

---

## 📊 Data Integration Strategy

### MongoDB Collections Used

1. **videos**
   - Fields: `_id`, `category`, `uploader`, `length_sec`, `age_days`
   - Usage: Video metadata, categorization, filtering

2. **edges**
   - Fields: `src`, `dst`
   - Usage: Graph construction, degree computation, PageRank

3. **video_snapshots**
   - Fields: `video_id`, `views`, `rate`, `ratings`, `comments`, `crawl_id`, `age_days`
   - Usage: Time-series data, popularity metrics, top-K queries

4. **crawls** (if available)
   - Fields: `_id`, `date`, `status`
   - Usage: Dataset metadata, temporal filtering

5. **degree_statistics** (computed)
   - Fields: `id`, `inDegree`, `outDegree`, `total_degree`
   - Usage: Pre-computed results for faster loading

### Data Loading Strategy

**Approach 1: Full Load (Small Datasets < 1M records)**
- Load entire collections into Spark DataFrames
- Cache frequently used DataFrames
- Fast query execution

**Approach 2: Sampling (Large Datasets > 1M records)**
- Load sampled data (user-configurable)
- Use MongoDB aggregation for pre-filtering
- Parallel loading with partitioning

**Approach 3: Lazy Loading**
- Load data only when needed
- Use Streamlit caching to persist results
- Clear cache when refreshing

**Recommended: Hybrid Approach**
- Sample for initial exploration
- Full load for final analysis
- User option to switch between modes

---

## ⚡ Performance Optimization

### Caching Strategy

**Streamlit Cache Levels:**

1. **Session State** (ephemeral, per user)
   - UI state (selected filters, K values)
   - Temporary results

2. **@st.cache_data** (persistent across runs)
   - Loaded DataFrames
   - Computed statistics
   - Query results
   - Expire after 1 hour or manual refresh

3. **@st.cache_resource** (shared resources)
   - Spark session
   - MongoDB client
   - Graph models

**Example:**
```python
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_videos_from_mongo():
    spark = get_spark_session()
    df = spark.read.format("mongodb")...
    return df.toPandas()

@st.cache_resource
def get_spark_session():
    return SparkSession.builder...getOrCreate()
```

### Spark Optimization

1. **Partition Management**
   - Repartition large DataFrames: `df.repartition(8)`
   - Coalesce small results: `df.coalesce(1)`

2. **Predicate Pushdown**
   - Filter early in MongoDB queries
   - Use Spark filters before joins

3. **Persistence**
   - Cache frequently used DataFrames: `df.cache()`
   - Unpersist when done: `df.unpersist()`

4. **Broadcast Joins**
   - For small lookup tables: `broadcast(df)`

5. **Adaptive Query Execution**
   - Enable AQE in Spark config
   - Automatic optimization of shuffles

### UI Responsiveness

1. **Loading Indicators**
   - `st.spinner()` for long operations
   - `st.progress()` for trackable tasks

2. **Asynchronous Operations**
   - Use `st.session_state` to track computation status
   - Allow user to continue browsing while processing

3. **Pagination**
   - Display results in pages (50-100 rows)
   - Lazy load additional pages

4. **Progressive Rendering**
   - Show summary statistics first
   - Load visualizations incrementally

---

## 🔒 Error Handling & Validation

### Error Categories

1. **Connection Errors**
   - MongoDB connection failed
   - Spark session initialization failed
   - Network timeout

   **Handling:**
   - Display clear error message
   - Provide troubleshooting tips
   - Offer retry button
   - Log full error for debugging

2. **Data Errors**
   - Empty dataset
   - Missing required fields
   - Invalid data types

   **Handling:**
   - Check data availability before queries
   - Validate required fields exist
   - Provide helpful error messages
   - Suggest data fixes

3. **Query Errors**
   - Invalid parameters
   - Query timeout
   - Out of memory

   **Handling:**
   - Validate inputs before execution
   - Set reasonable timeouts
   - Suggest smaller sample sizes
   - Catch Spark exceptions gracefully

4. **User Input Errors**
   - Invalid ranges
   - Empty selections
   - Conflicting filters

   **Handling:**
   - Client-side validation
   - Disable submit until valid
   - Clear error messages
   - Preserve valid inputs

### Validation Rules

**Input Validation:**
- K value: `1 ≤ K ≤ 1000`
- Sample size: `100 ≤ size ≤ 1,000,000`
- Duration range: `0 ≤ min < max`
- PageRank probability: `0 < p < 1`
- Max iterations: `1 ≤ iter ≤ 100`

**Data Validation:**
- Check for null values
- Verify data types
- Ensure non-empty results
- Validate field existence

---

## 📈 Testing Strategy

### Unit Testing
- Test utility functions (`spark_connector`, `mongo_connector`)
- Test data processing functions
- Test visualization generation
- Use `pytest` framework

### Integration Testing
- Test MongoDB → Spark data flow
- Test Spark → Pandas conversion
- Test end-to-end query execution
- Verify caching behavior

### UI Testing
- Manual testing of all pages
- Test all input combinations
- Verify error handling
- Cross-browser testing (Chrome, Firefox, Edge)

### Performance Testing
- Test with different dataset sizes
- Measure query execution times
- Monitor memory usage
- Identify bottlenecks

### User Acceptance Testing
- Test with real usage scenarios
- Verify all requirements met
- Collect feedback from team
- Document any issues

---

## 📦 Deployment & Delivery

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export MONGO_URI="mongodb://localhost:27017/youtube_analytics"
export JAVA_HOME="C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"

# Run application
streamlit run app/main.py
```

### Configuration Files

**requirements.txt:**
```
streamlit>=1.28.0
pyspark>=3.5.0
pymongo>=4.6.0
pandas>=2.1.0
plotly>=5.18.0
matplotlib>=3.8.0
networkx>=3.2.0
graphframes>=0.8.4
numpy>=1.24.0
```

**`.streamlit/config.toml`:**
```toml
[theme]
primaryColor = "#FF4B4B"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"

[server]
port = 8501
headless = false
enableCORS = false
enableXsrfProtection = true

[browser]
gatherUsageStats = false
```

### Deliverables Checklist

**Code:**
- [ ] Complete source code in `app/` directory
- [ ] All required dependencies in `requirements.txt`
- [ ] Configuration files
- [ ] Git repository with clear commit history

**Documentation:**
- [ ] This design plan (Milestone4_GUI_Design_Plan.md)
- [ ] User guide (Milestone4_User_Guide.md)
- [ ] Technical documentation (Milestone4_Technical_Docs.md)
- [ ] Code comments and docstrings
- [ ] README updates

**Demonstration:**
- [ ] Demo video (3-5 minutes)
- [ ] Screenshots of all major features
- [ ] Sample queries and results
- [ ] Performance metrics

**Presentation:**
- [ ] PowerPoint slides
- [ ] Live demo preparation
- [ ] Q&A preparation

---

## 🎓 Learning Outcomes & Requirements Alignment

### Milestone 4 Requirements Met

✅ **GUI Development**
- Multi-page Streamlit web application
- Interactive user interface
- Professional design and UX

✅ **MongoDB Integration**
- Direct connection to MongoDB Atlas/local
- Data loading and querying
- Collection management

✅ **Spark Integration**
- PySpark session management
- Execution of all analytics algorithms
- GraphFrames for graph analysis

✅ **Analytics Features**
- Network aggregation (degree distribution)
- Categorized statistics
- Top-K queries
- Range queries
- Pattern search (motifs)
- Influence analysis (PageRank)

✅ **Visualization**
- Interactive charts (Plotly)
- Data tables with sorting/filtering
- Network graphs
- Statistical summaries

✅ **Usability**
- Intuitive navigation
- Clear feedback and error messages
- Export functionality
- Configuration options

### Course Learning Objectives

1. **Big Data Storage:** MongoDB document store for large datasets
2. **Distributed Processing:** Apache Spark for scalable analytics
3. **Graph Analytics:** GraphFrames for network analysis
4. **NoSQL Integration:** Direct database-to-processing pipeline
5. **Application Development:** End-to-end data application with GUI
6. **Performance Optimization:** Caching, sampling, and query optimization

---

## 📚 References & Resources

### Documentation
- [Streamlit Documentation](https://docs.streamlit.io/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [MongoDB Python Driver](https://pymongo.readthedocs.io/)
- [GraphFrames Guide](https://graphframes.github.io/graphframes/docs/_site/index.html)
- [Plotly Python Documentation](https://plotly.com/python/)

### Project Resources
- Dataset: [YouTube Statistics and Social Network](http://netsg.cs.sfu.ca/youtubedata/)
- Project Description: `Docs/ProjectDescription.md`
- Milestone 2: Completed (data loading & database setup)
- Milestone 3: Completed (Spark algorithms implementation)

### Development Tools
- Python 3.11+
- Apache Spark 3.5.x
- MongoDB 7.x
- Streamlit 1.28+
- Visual Studio Code
- Git/GitHub

---

## 👥 Team Responsibilities

### Ross Kugler (Data Pipeline & API Lead)
- Spark connector implementation
- Query engine development
- MongoDB integration
- Performance optimization
- API design (if time permits)

### Huy (Harry) Ky (Database Manager)
- MongoDB configuration and management
- Data validation and quality checks
- Collection optimization
- Database documentation
- Data loading utilities

### Ben Bordon (Analytics & Algorithms Lead)
- Algorithm integration into GUI
- PageRank implementation refinement
- Visualization development
- Statistical analysis features
- Documentation and user guide

**Shared Responsibilities:**
- UI/UX design and implementation
- Testing and debugging
- Demo preparation
- Presentation development

---

## 📝 Notes & Considerations

### Assumptions
1. MongoDB database is already populated with YouTube data (from Milestone 2)
2. Spark algorithms are functional and tested (from Milestone 3)
3. Development environment has Java 17 and sufficient memory (4GB+)
4. Network connection available for MongoDB Atlas access

### Potential Challenges
1. **Large Dataset Performance**
   - Mitigation: Implement sampling and caching
   - Use progress indicators for long operations

2. **Graph Visualization Scalability**
   - Mitigation: Limit displayed nodes (top 100)
   - Use force-directed layout with constraints

3. **Spark Memory Issues**
   - Mitigation: Configure appropriate memory limits
   - Implement memory monitoring

4. **UI Responsiveness**
   - Mitigation: Use async operations where possible
   - Implement lazy loading

### Future Enhancements (Post-Milestone 4)
- REST API for programmatic access
- Cloud deployment (Streamlit Cloud, AWS, Azure)
- Real-time data updates
- User authentication and saved queries
- Advanced graph visualizations (3D, interactive)
- Machine learning predictions
- Comparative analysis across crawls
- Export to multiple formats (PDF reports, Excel)

---

## ✅ Success Metrics

### Technical Metrics
- [ ] All pages load within 3 seconds (initial)
- [ ] Queries execute within 30 seconds (sampled data)
- [ ] No critical bugs or crashes
- [ ] 100% of analytics features implemented
- [ ] All data properly visualized

### User Experience Metrics
- [ ] Intuitive navigation (< 3 clicks to any feature)
- [ ] Clear error messages with actionable advice
- [ ] Consistent UI across all pages
- [ ] Professional appearance
- [ ] Helpful tooltips and documentation

### Project Metrics
- [ ] Meets all Milestone 4 requirements
- [ ] Delivered on time
- [ ] Complete documentation
- [ ] Successful demonstration
- [ ] Team collaboration effective

---

## 🏁 Conclusion

This design plan provides a comprehensive roadmap for developing the YouTube Analyzer GUI application for Milestone 4. By leveraging Streamlit for rapid UI development and integrating with our existing MongoDB and Spark infrastructure, we will deliver a professional, functional, and user-friendly analytics platform.

The multi-page architecture allows for clear separation of concerns, while the shared utility modules ensure code reusability and maintainability. The phased implementation approach prioritizes core functionality first, with advanced features and polish in later phases.

With this plan, our team is well-positioned to successfully complete Milestone 4 and deliver an impressive end-to-end big data application.

---

**Document Version:** 1.0  
**Last Updated:** November 18, 2025  
**Status:** Ready for Implementation
