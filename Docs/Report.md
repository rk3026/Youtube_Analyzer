# YouTube Network Analyzer – Final Project Report (Draft)

## 1. Problem Statement

Online video platforms such as YouTube generate massive volumes of content and user interactions every day. While YouTube offers basic analytics for content creators (views, likes, comments, watch time), these views mostly focus on individual videos or channels. They provide limited insight into the *network structure* that emerges between videos, categories, and channels through recommendations, co‑viewing, and topical similarity.

Our project addresses the following problem:

> How can we model YouTube videos and their relationships as a large‑scale graph, and provide interactive, scalable analytics on this graph to help users understand video networks, influential content, category‑level patterns, and recommendation structures?

This problem is important for several reasons:

- **Understanding information diffusion.** Video networks reveal how information, trends, and misinformation can propagate via recommendations and related content links.
- **Content discovery and curation.** Analyzing the structure of video networks can help identify influential videos or channels, diverse content clusters, and under‑served niches.
- **Platform and user transparency.** Visual and quantitative analytics over the recommendation network can shed light on how users are steered toward particular content.
- **Big data engineering practice.** YouTube data is inherently large‑scale, noisy, and semi‑structured, making it an excellent domain to evaluate big data architectures involving distributed processing and NoSQL storage.

Our goal is to design and implement a **YouTube Network Analyzer** that:

1. Ingests and cleans real YouTube metadata and relationship information.
2. Stores this data in a scalable NoSQL database (MongoDB).
3. Uses PySpark to process the data and build a network representation.
4. Computes network statistics and supports interactive queries (top‑K, range queries, pattern search, and graph analytics).
5. Exposes these capabilities through a user‑friendly Streamlit web interface.

---

## 2. Team

Our team consists of <N> members. Each member took responsibility for specific components, while collaborating on design decisions and integration testing.

- **<Member A> – Data Engineering Lead**  
  - Designed the data schema and MongoDB collections.  
  - Implemented data ingestion and processing pipelines (`process_crawl_data.py`).  
  - Responsible for data cleaning, normalization, and loading into MongoDB and Spark.

- **<Member B> – Big Data & Algorithms Lead**  
  - Implemented PySpark‑based analytics scripts (e.g., `Spark/Degree_distribution_Categorized_Statistics.py`, `scripts/spark-basic.py`).  
  - Designed and implemented top‑K queries, range queries, and network statistics.  
  - Evaluated algorithm performance and scalability on larger datasets.

- **<Member C> – Backend & Integration Engineer**  
  - Implemented the MongoDB and Spark connectors in `app/utils/mongo_connector.py` and `app/utils/spark_connector.py`.  
  - Designed the query APIs consumed by the Streamlit UI (e.g., `app/analytics/*.py`).  
  - Ensured reliable communication between the UI, MongoDB, and Spark components.

- **<Member D> – UI/UX & Visualization Lead**  
  - Designed and implemented the Streamlit application in `app/Home.py`, `app/pages`, and `app/components`.  
  - Built visual analytics components such as network visualizations, metric dashboards, and interactive filters.  
  - Responsible for user flows, instructions, and high‑level documentation (e.g., GUI design in Milestone 4).

(Adjust roles and names according to your actual team.)

---

## 3. Related Work

Our project is inspired by and builds upon several areas of prior work:

1. **YouTube and Online Video Analytics.**  
   Prior research has characterized YouTube as a social and information network, focusing on video popularity, recommendation bias, and community structures. These works demonstrate that graph representations (videos as nodes, edges as recommendations or co‑viewing relationships) capture important structural properties such as degree distributions, clustering, and community formation.

2. **Network Science and Graph Analytics.**  
   Techniques from network science—such as degree distributions, centrality measures, connected components, and community detection—are commonly applied to social and information networks. Many open‑source frameworks (e.g., GraphX, GraphFrames, NetworkX) support such analytics. Our project adapts these concepts to YouTube video graphs using PySpark and graph‑oriented computations.

3. **Big Data Systems: Spark and NoSQL Databases.**  
   Apache Spark has become a de facto standard for large‑scale data processing, especially when handling semi‑structured datasets (e.g., JSON) at scale. NoSQL databases like MongoDB are widely used for storing and querying flexible document schemas. Existing literature and documentation show patterns for combining Spark and MongoDB for ETL and analytic workloads.

4. **Visual Analytics and Dashboards.**  
   Visualization libraries and platforms (e.g., Streamlit, Dash) are commonly used to wrap complex analytics in interactive dashboards. Our GUI design builds on best practices discussed in dashboard design literature and official Streamlit documentation, focusing on clarity, responsiveness, and task‑oriented layouts.

During the project we referenced:

- YouTube Data API documentation for understanding available fields and relationships.
- Official documentation for Apache Spark and PySpark for distributed operations and performance considerations.
- MongoDB documentation and tutorials for schema design and indexing strategies.
- Streamlit documentation and community examples for building multi‑page analytic dashboards.
- Academic papers on YouTube network analysis and recommendation systems (see References section).

Our YouTube Network Analyzer is not a novel algorithmic contribution by itself; instead, it integrates these ideas into an end‑to‑end, working big data application that emphasizes **data pipeline design**, **scalable analytics**, and **user‑centric visualization**.

---

## 4. Big Data Solution

### 4.1 Overall Architecture

The system architecture follows a modular design with four major layers:

1. **Data Preparation Layer** — Collects and preprocesses YouTube data into a consistent schema.
2. **Database Layer (MongoDB)** — Stores cleaned video documents, relationships, and derived statistics.
3. **Analytics Layer (PySpark / Spark)** — Computes network statistics, top‑K queries, range queries, and pattern searches over large datasets.
4. **User Interface Layer (Streamlit)** — Provides an interactive web interface for network statistics, visual analytics, and exploration.

The repository structure reflects this separation:

- Data preparation: `process_crawl_data.py`  
- Spark analytics: `Spark/`, `scripts/`  
- Database utilities: `app/utils/`  
- Application logic and GUI: `app/Home.py`, `app/pages/`, `app/components/`  
- Documentation: `Docs/`

The system is designed to run either in a local single‑node Spark mode or against a small Spark cluster, controlled by PowerShell scripts such as `start-spark-only.ps1`, `start-with-spark.ps1`, and `start.ps1`.

### 4.2 Data Preparation

Raw YouTube data originates from a prior crawling process (described in Milestones 1–3). The key tasks in data preparation are:

- **Parsing and Cleaning.**  
  The script `process_crawl_data.py` reads raw JSON/CSV exports from YouTube crawls. It:
  - Normalizes video IDs, channel IDs, and category IDs.  
  - Parses timestamps, durations, and numeric fields (views, likes, comments).  
  - Handles missing or inconsistent fields by applying default values or dropping unusable records.

- **Network Construction.**  
  We derive network edges using:
  - **Related or Recommended Videos:** Edges from a video to each recommended video.  
  - **Category and Channel Relationships:** Groupings of videos by category/channel to support category‑level and channel‑level statistics.  
  - **Pattern or Keyword Links (when available):** Edges formed via shared tags or title/description keywords for pattern search.

- **Schema Mapping for MongoDB.**  
  After cleaning, records are mapped to the MongoDB schema described in `Docs/MongoDB_schema.md`. Each video document typically includes:
  - Video metadata (ID, title, description, publish time, channel, category).  
  - Engagement statistics (views, likes, dislikes if present, comment count).  
  - Network relationships (lists of related/recommended video IDs).  
  - Precomputed or derived fields used by analytics (e.g., degree counts for caching).

- **Batch Insertion and Indexing.**  
  Processed data is inserted into MongoDB with indexes on fields such as `video_id`, `channel_id`, `category_id`, and engagement metrics. These indexes support efficient point lookups and range queries from the UI.

This stage transforms raw, noisy crawled data into a consistent, queryable foundation suitable for both Spark and MongoDB analytics.

### 4.3 Database System

We use **MongoDB** as the primary storage for video metadata and network information. The design is optimized for:

- Flexible, semi‑structured documents (YouTube metadata is not strictly uniform).
- Fast retrieval for interactive queries.
- Integration with PySpark for batch/statistical processing.

Key aspects:

- **Collections and Documents.**  
  The main collection holds video documents. Additional collections (if used) may store category statistics or preaggregated metrics, but the predominant model is a single rich document per video that includes relationships.

- **Indexes.**  
  Indexes are chosen based on anticipated query patterns:
  - `video_id` (unique, for direct lookup).  
  - `category_id`, `channel_id` (for category/channel level aggregations).  
  - Engagement metrics (e.g., views) to speed up top‑K queries within MongoDB, when used directly from the UI.  
  - Optional compound indexes for frequent filters (e.g., category + views).

- **Connector Implementation.**  
  The module `app/utils/mongo_connector.py` encapsulates MongoDB connection creation and retrieval. The main app and analytics components access MongoDB only through this connector, which:
  - Reads connection parameters (host, port, database name, credentials) from configuration or environment.  
  - Exposes helper functions to query collections and handle exceptions consistently.

MongoDB is well‑suited for our workload because data updates are mostly batched (from the data preparation scripts), while reads are frequent, diverse, and often filter‑based.

### 4.4 Queries and Algorithms

The analytics layer uses **PySpark** (and when appropriate, MongoDB aggregations) to implement several classes of queries. Much of the PySpark logic is reflected in files under `Spark/` and `scripts/`, and wrapped for UI consumption under `app/analytics/`.

We focus on four main analytics types, corresponding to the Streamlit pages:

1. **Network Statistics (Page `1_Network_Statistics.py`).**  
   Implemented with helper modules such as:
   - `app/analytics/degree_stats.py`  
   - `app/analytics/category_stats.py`  
   - `app/components/network_metrics.py`

   These compute:
   - Degree distributions (in‑degree, out‑degree) of videos in the recommendation graph.  
   - Category‑wise counts, averages, and histograms.  
   - Basic graph metrics (e.g., maximum degree, average degree, distribution shape).

   PySpark jobs read data either directly from MongoDB (via the connector) or from intermediate Parquet/CSV files, then perform `groupBy`, `agg`, and window functions to derive statistics. For example, degree distributions are computed by aggregating the number of outgoing edges (related videos) for each node.

2. **Top‑K Queries (Page `2_Top_K_Queries.py`).**  
   Implemented in `app/analytics/topk_queries.py`. Typical queries include:
   - Top‑K videos by view count, likes, or comment count, optionally filtered by category or channel.  
   - Top‑K most connected (highest degree) videos in the network.

   In Spark, this is done using `orderBy(..., ascending=False)` with `limit(K)` or via window functions for partitioned top‑K (e.g., top‑K per category). When the dataset size is small enough, MongoDB’s aggregation pipeline with `$sort` and `$limit` is also viable; our design allows either pattern.

3. **Range Queries (Page `3_Range_Queries.py`).**  
   Implemented in `app/analytics/range_queries.py`. Examples:
   - Videos with view counts within a specified range.  
   - Videos published within a date interval.  
   - Videos whose engagement statistics (e.g., likes ratio) fall between two thresholds.

   These align well with Spark’s filter operations and MongoDB’s range queries indexed on numeric or date fields. The UI allows users to specify lower and upper bounds through sliders or input boxes, which are then translated into filter predicates.

4. **Graph Analytics (Page `4_Graph_Analytics.py`).**  
   Implemented in modules like `app/analytics/degree_stats.py` and visualized via `app/components/graph_visualization.py`. We focus on:
   - Degree‑based metrics and distributions.  
   - Category‑level networks (e.g., edges aggregated by category).  
   - Exploration of local neighborhoods around selected nodes (e.g., ego networks).

   While we do not implement full distributed graph algorithms such as PageRank or community detection due to time and resource constraints, our architecture is compatible with extending to Spark GraphFrames or GraphX in the future.

5. **Pattern Search (Page `5_Pattern_Search.py`).**  
   Implemented in `app/analytics/pattern_search.py`. This feature:
   - Allows keyword search over video titles, descriptions, and tags.  
   - Supports simple pattern queries such as “find videos whose title contains a given phrase and whose view count exceeds a threshold.”  
   - Combines text matching with numeric filters to enable exploratory analysis of specific topics.

The `app/analytics` modules present a clean interface to the UI: each function receives query parameters (category, thresholds, K, keywords, etc.), performs the necessary Spark or MongoDB operations, and returns results as pandas DataFrames for rendering.

### 4.5 User Interface

The user interface is implemented as a **multi‑page Streamlit application**, located under `app/`. The main entry point is `app/Home.py`, and additional pages are under `app/pages/`. Components in `app/components/` encapsulate reusable UI elements.

Key design features:

- **Home Page (`Home.py`).**  
  - Sets up the global layout using `st.set_page_config` with a wide layout and sidebar navigation.  
  - Renders a main header and a short description: “YouTube Network Analyzer – Big Data Analytics Platform for YouTube Video Networks”.  
  - Shows connection status via `render_connection_status()` to inform the user about MongoDB connectivity.  
  - Displays dataset overview metrics via `render_dataset_overview(mongo)`, providing high‑level counts and summary statistics.  
  - Presents a visual analytics summary using `render_visual_analytics(mongo)` and shows project instructions via `render_instructions()`.

- **Reusable Components (`app/components/`).**  
  - `connection_status.py`: checks MongoDB availability and shows user‑friendly messages.  
  - `dataset_overview.py`: displays key metrics such as number of videos, categories, and channels.  
  - `network_metrics.py`: visualizes network statistics (degree distributions, etc.).  
  - `graph_visualization.py`: displays subgraphs or neighborhood structures for selected nodes.  
  - `quick_actions.py` and `visual_analytics.py`: provide shortcuts to common queries and visualizations.  
  - `instructions.py`: shows guidance on how to use each part of the application.

- **Analytics Pages (`app/pages/`).**  
  Each page corresponds to a major analytic capability:
  - `1_Network_Statistics.py`: metrics and plots summarizing network structure.  
  - `2_Top_K_Queries.py`: configurable top‑K queries by views, likes, or degree.  
  - `3_Range_Queries.py`: sliders and controls for specifying numeric/date ranges.  
  - `4_Graph_Analytics.py`: graph‑focused visualizations and metrics.  
  - `5_Pattern_Search.py`: text‑based search and result exploration.

The UI emphasizes:

- **Responsiveness.** Streamlit’s layout primitives (`columns`, `expander`, etc.) are used to keep visualizations and selectors organized.
- **Clarity.** Each page is titled clearly and includes short explanatory text so users understand what queries they are executing.
- **Error Handling.** When MongoDB or Spark are unavailable, the UI shows informative warnings rather than failing silently.

The PowerShell scripts (`start.ps1`, `start-with-spark.ps1`, `start-streamlit-only.ps1`) simplify starting the full stack (Spark + Streamlit) or the UI alone.

---

## 5. Results and Findings

### 5.1 Sample Results

We evaluated our system using the crawled YouTube dataset prepared in earlier milestones. The dataset contains <N> videos across <M> categories and <K> channels (fill in actual numbers).

Representative sample results include:

1. **Degree Distributions.**  
   - The out‑degree distribution (number of recommended videos per node) shows a right‑skewed pattern: most videos have a small number of outgoing edges, while some have many, consistent with “hub” behavior.  
   - The in‑degree distribution (how many times a video is recommended by others) is even more skewed, highlighting a small set of highly recommended videos.

2. **Category Statistics.**  
   - Certain categories (e.g., Music, Entertainment) dominate in both video count and cumulative views.  
   - Educational categories may have fewer videos but relatively higher average watch metrics, indicating more targeted audiences.

3. **Top‑K Queries.**  
   - Top‑K videos by views and likes often overlap but can differ from top‑K by in‑degree, suggesting that popular videos are not always the most central in the recommendation network.  
   - For specific categories, the top‑K list reveals both expected mainstream hits and niche but highly connected videos.

4. **Range Queries.**  
   - Range queries by publish date show a clear growth in video counts over time and can surface early‑adopter content in newer categories.  
   - Filtering by moderate view ranges (e.g., mid‑tier videos) highlights content that may be good candidates for discovery but not yet viral.

5. **Pattern Search.**  
   - Keyword searches (e.g., “tutorial”, “review”, or specific product names) often produce clusters of related videos with similar metadata and engagement profiles.  
   - Combining keyword filters with view or date ranges helps identify emerging trends.

Where appropriate, we render these results as:

- Bar charts and histograms for distributions.
- Line charts for time‑based statistics.
- Network diagrams for localized graph structures.

### 5.2 Accuracy, Reliability, and Scalability

We assess our solution along three dimensions:

#### Accuracy

- **Data Quality Checks.**  
  During data preparation, we performed sanity checks on key fields (non‑null IDs, non‑negative view counts, consistent date formats). Erroneous records are either corrected (if possible) or dropped.

- **Cross‑Checking Counts.**  
  We verified counts derived from Spark operations against MongoDB aggregations for small subsets, ensuring that results match across systems.

- **Deterministic Computations.**  
  Our analytics rely primarily on deterministic aggregations (sum, count, average, sort) rather than approximate algorithms, so given the same data, results are reproducible.

#### Reliability

- **Robust Connectors.**  
  The MongoDB connector (`get_mongo_connector` in `app/utils`) handles connection errors and exposes them clearly to the UI. The app’s `render_connection_status()` component surfaces connection state to users.

- **Graceful Degradation.**  
  If MongoDB is unreachable or Spark is not running, the UI pages display warnings instead of crashing. This behavior is demonstrated in `Home.py`, which wraps MongoDB access in a `try/except` block and logs errors.

- **Logging.**  
  The main app configures logging via Python’s `logging` module, allowing us to inspect errors and performance issues during development and testing.

#### Scalability

- **Distributed Processing with Spark.**  
  By running analytics in PySpark, we leverage Spark’s ability to distribute computations across multiple cores or nodes. Operations such as `groupBy`, joins, and window functions scale well with data size.

- **NoSQL Database Choice.**  
  MongoDB is well‑suited for scaling reads and writes horizontally. As data grows, we can add shards and replicas to maintain performance.

- **UI Responsiveness Strategies.**  
  For very large datasets, the UI can rely on pre‑aggregated tables and cached Spark results instead of re‑computing heavy analytics on every request. While we did not fully implement caching in this milestone, the separation between analytics functions and UI makes it straightforward to add.

Empirically, on our available hardware and dataset size (on the order of <X> GB and <Y> videos; fill in your actual numbers), the system responds interactively for common queries (top‑K, range queries, basic statistics). More complex graph analytics and visualizations may take longer but remain within acceptable limits for exploratory use.

---

## 6. Conclusion

In this project, we designed and implemented the **YouTube Network Analyzer**, an end‑to‑end big data platform for exploring YouTube video networks. Our system:

- Models YouTube videos and their relationships as a graph captured in MongoDB.
- Uses PySpark to compute network statistics, top‑K results, range‑based analytics, and pattern‑based searches at scale.
- Exposes these capabilities through a multi‑page Streamlit web application with modular components, interactive controls, and visualization.

Through our experiments, we observed meaningful structural patterns in the video network, such as skewed degree distributions, dominant categories, and differences between popularity and centrality. The project demonstrates how widely used big data technologies (Spark, MongoDB, Streamlit) can be combined to produce a practical analytics tool for complex, semi‑structured data.

There are several directions for future work:

- Integrating more advanced graph algorithms (PageRank, community detection) using GraphFrames or GraphX.
- Incorporating temporal analysis of recommendation changes over time.
- Enhancing the pattern search engine with full‑text search indices and more powerful query language.
- Scaling out to larger, multi‑node clusters and evaluating performance on significantly larger datasets.

Overall, the project met our initial goals and provided valuable hands‑on experience with the complete big data lifecycle: from data ingestion and cleaning, through distributed analytics, to user‑facing visualization.

---

## 7. References

(Replace placeholders with actual citation formats used in your course—APA, IEEE, etc.)

1. YouTube Data API v3 Documentation, Google Developers.  
2. Apache Spark Documentation, The Apache Software Foundation.  
3. MongoDB Manual, MongoDB Inc.  
4. Streamlit Documentation, Streamlit Inc.  
5. <Author(s)>, “Analyzing YouTube: A Large‑Scale Measurement Study of Video Content and User Behavior,” <Conference/Journal>, <Year>.  
6. <Author(s)>, “The YouTube Video Recommendation System: Design and Impact,” <Venue>, <Year>.  
7. <Any other academic papers or online resources you cited in Milestones 1–3>.  
