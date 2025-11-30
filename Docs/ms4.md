# Milestone 4 — GUI / Scalability Report

This report summarizes the YouTube Analyzer application UI, interactive features, user queries and results, and scalability details discovered by inspecting the repository and existing runtime logs.

---

## 1) User Interface and Data Visualization 🧭

The application is implemented with Streamlit. The UI is split across multi-page flows in `app/pages` with auxiliary components in `app/components`.

Main entry point: `app/main.py` sets the layout and imports helper components:
- `render_connection_status`, `render_dataset_overview`, `render_visual_analytics`, `render_instructions` from `app/components`.
- `main.py` provides header, dataset overview, and access to pages via Streamlit's multipage directory.

Files implementing UI pages (numeric ordering in `app/pages`):
- `app/pages/1_Network_Statistics.py` — Degree distribution and categorized stats
- `app/pages/2_Top_K_Queries.py` — Top K queries (categories, most viewed, highest rated)
- `app/pages/3_Range_Queries.py` — Duration & Views range filters
- `app/pages/4_Graph_Analytics.py` — Graph analytics (PageRank, community detection, centrality, visualization)
- `app/pages/5_Pattern_Search.py` — Pattern Search (related pairs, chains, common recommendations)

Key UI structure and interactive elements:
- Multi-page navigation creates separate workflows for statistics, top-k, range queries, and graph analytics.
- Tabs on pages (e.g., `st.tabs`) partition related analysis: degree distribution vs categorized statistics; top categories vs most viewed vs highest rated; duration vs view range; PageRank vs community vs centrality vs visualization.
- User inputs and controls (all interactive inputs use Streamlit widgets):
  - Sliders: `sample size`, `k` for top-k, `duration` ranges, `views` ranges, `min ratings`, PageRank `resetProbability`, and iteration inputs (e.g., `st.slider`, `st.number_input`) in pages.
  - Selectboxes and drop-downs: Select a category from the `videos` collection, select layout algorithm and color mode for graph visualization (`st.selectbox`).
  - Buttons: Action triggers for queries (e.g., `Run Analysis`, `Find Top Categories`, `Search`, `Run PageRank`, `Generate Visualization` etc).
  - Checkboxes: Options like `Show Labels` on nodes.
  - Number Inputs: For PageRank iterations and sample sizes for some graph algorithms.
  - Metrics and quick feedback: `st.metric` and `st.info` are used to show summary stats.
  - Save/archive buttons: To save results to a `youtube_analytics_archives` database using MongoDB.

Data visualization components:
- Plotly express charts: `px.bar`, `px.pie`, `px.histogram`, and `px.scatter` are used extensively for bar charts, pies, histograms, and scatter plots.
- Plotly + networkx (Plotly `go.Figure`): Interactive network visualizations are implemented, constructing a node/edge layout and rendering nodes/edges with color and size mapping.
- Tables and clickable links: The UI converts video IDs to clickable YouTube links using helpers in `utils.youtube_helpers.py` and renders them as HTML or Streamlit tables. A typical pattern: convert ID -> `https://youtu.be/<id>`, show as HTML link in table.
- Summaries: Abrupt summary metrics (Avg Degree, Max Degree, Min Degree, Std Dev) appear using `st.metric` for quick high-level overviews.

UI design considerations:
- Streamlit caching is used for resources: `get_mongo_connector` & `get_spark_connector` use `st.cache_resource` to reuse connections across reruns.
- Session state is used to store analysis results, enabling the user to run a query and review results across interactions without rerunning computations.
- Performance/UI design (from `Docs/Milestone4_GUI_Design_Plan.md`): progressive rendering, sampling options, and caching are proposed to keep UI responsive (default sample sizes are user-configurable, e.g., 50k edges by default).

---

## 2) User Queries and Results 🔎

All user queries are implemented as Spark-based analytics modules in `app/analytics`. The UI pages call analytics APIs and render results.

Main modules and their queries:
- `analytics/DegreeAnalytics`  (`app/analytics/degree_stats.py`):
  - compute_out_degrees, compute_in_degrees
  - compute combined degree stats (total_degree)
  - get top-k by degree (total/in/out)
  - get degree distribution histogram (group by total_degree)
  - run_full_analysis(sample_size): loads `edges` collection (optionally samples); returns:
    - `aggregate_stats` — numeric summary of graph degree stats
    - `top_videos` — list of top videos by degree (IDs, in/out), and
    - `distribution` — counts per degree value
  - UI page: `1_📊_Network_Statistics.py` provides sliders (sample size), runs the analysis, displays summary metrics and Plotly charts, and allows saving results to `youtube_analytics_archives`.

- `analytics/CategoryAnalytics` (`app/analytics/category_stats.py`):
  - compute_category_distribution() — top categories by number of videos
  - compute_length_distribution() — bucketed length categories (0–2m, 2–5m, 5–10m, …)
  - compute_view_distribution() — bucketed view counts (0–1K, 1K–10K, …)
  - run_full_analysis(sample_size) — uses `videos` or sampled `video_snapshots` to produce:
    - `by_category`, `by_length`, `by_views` lists of dicts for UI plotting
  - UI: See `1_📊_Network_Statistics.py` (Categorized Statistics tab). Shows Plotly bar/pie charts.

- `analytics/TopKAnalytics` (`app/analytics/topk_queries.py`):
  - get_top_k_categories(k) — group by `category`, return top k
  - get_top_k_most_viewed(k) — order `video_snapshots` by `views` (most recent snapshot data), return top k video records
  - get_top_k_highest_rated(k, min_ratings) — filter snapshots by `ratings >= min_ratings`, order by `rate` (rating), return top k. (UI provides `min_ratings` slider.)
  - UI: `2_🔍_Top_K_Queries.py` shows charts and tables. Results include clickable video links, counts, categories.

- `analytics/RangeQueryAnalytics` (`app/analytics/range_queries.py`):
  - query_by_duration(category, min, max, max_results) — filter `video_snapshots` by `length_sec` between min/max and selected category; returns a list of video records (duration, views, rating)
  - query_by_views(min_views, max_views, category, max_results) — filter `video_snapshots` by view count range (optionally category) and returns results ordered by views
  - UI: `3_🎯_Range_Queries.py` implements user inputs and displays results in tables and histograms, pie charts.

  - `analytics/PatternSearch` (`app/analytics/pattern_search.py`):
    - `find_related_video_pairs(category, max_results, sample_size)` — list of source/related pairs
    - `find_video_chains(category, chain_length, max_results)` — list of chains
    - `find_common_recommendations(category, min_common, max_results)` — list of common recommendation targets
    - UI: `5_Pattern_Search.py` implements selectors (category, pattern type, sample size), displays tables and Plotly bar charts, and allows saving results to `youtube_analytics_archives`.

Graph Algorithms and queries (Graph Analytics page): `4_Graph_Analytics.py`
- PageRank using GraphFrames (GraphFrame.pageRank). The page loads `edges` into Spark and builds vertices; runs PageRank and returns `top_videos` by PageRank score; UI shows ranked bar chart and histogram.
- Community Detection: Using GraphFrame.labelPropagation(maxIter) to find community labels; groups by label and returns community sizes for plotting.
- Centrality Metrics: Implemented with `networkx` using the sampled edge set; computes:
  - in_degree, out_degree, total_degree
  - betweenness (approx using `k=min(100, node_count)` in the `nx.betweenness_centrality` call)
  - closeness suite (with fallback if fails)
  - UI shows top-ranked videos and scatter plots for centrality comparisons.
- Network/Interactive Visualization: `networkx` and Plotly are used to create node/edge traces; user can choose layouts (Spring, Circular, Kamada-Kawai, Shell), color by Degree/PageRank/Category, sample size for nodes/edges and show or hide labels. UI shows a Plotly chart and metrics about nodes, edges, density, and average degree.

### Pattern Search (New page: `5_Pattern_Search.py`)
- Purpose: Finds subgraph patterns using DataFrame joins (avoids GraphFrames for stability): related pairs (a→b), chains (a→b→c), and common recommendations (a→c←b).
- Inputs: category filter, pattern type selector (Related Pairs, Video Chains, Common Recommendations), sample size, max results.
- Analytics module: `app/analytics/pattern_search.py` — functions:
  - `find_related_video_pairs(category=None, max_results=50, sample_size=50000)` → records with `source_video` and `related_video` fields
  - `find_video_chains(category=None, chain_length=2, max_results=30)` → records with `video_a`, `video_b`, `video_c` fields
  - `find_common_recommendations(category=None, min_common=2, max_results=30)` → records with `video_a`, `video_b`, `common_target`
- Outputs: Streamlit tables and Plotly visualizations (bar charts of top sources, bridge videos, commonly recommended targets). Results may be saved to `youtube_analytics_archives` with a timestamped collection name.

Data exported/saved by UI:
- Results can be saved to MongoDB in the `youtube_analytics_archives` database. `app/pages/*` provide save buttons which persist results as timestamped collections.

---

## 3) Scalability 🔧

The source code provides a solid, documented local clustering setup (Spark local multi-core) and supports configuration options that allow the application to scale in a cluster deployment with a few modifications. The repository includes a `SparkConnector` with recommended Spark configs and a `MongoDBConnector` that supports local and Atlas URIs.

### 3.a Data storage in MongoDB (NoSQL) — cluster considerations

Repository data model (collections used):
- `videos` — documents per video (metadata: category, uploader, length, etc.)
- `edges` — each document includes `src` and `dst` (graph edges)
- `video_snapshots` — time-series snapshot per video: `video_id`, `views`, `rate`, `ratings`, `crawl_id` (snapshot/time), etc.
- `degree_statistics` (computed) — optional pre-computed statistics; used for faster retrieval in dashboards
- `youtube_analytics_archives` — used to store saved/archived results from UI runs

Cluster storage recommendations (how to shard and index):
- Shards/Replication: Configure a MongoDB Sharded Cluster (3+ shards) with replica sets for high availability. Use a replica set per shard for durability and fail-over.
- Shard key recommendations:
  - `videos` or `video_snapshots` can be sharded using `video_id` or `crawl_id` combined with `video_id` (a hashed `video_id` helps distribute uniform key distribution for large datasets such as video snapshots).
  - `edges` might best be sharded by `src` (or `src` hashed) to spread read-load across shards; however, consider query patterns: if most queries filter by `dst`, consider `dst` as shard key or use a compound key like `{src: 1, dst: 1}` depending on dominant access patterns.
- Indexing: create indices to support heavy read workloads:
  - `edges` collection: add indexes on `src` and `dst` (single field indexes) to make degree computations fast. If sharded by `src`, index `src` if not default.
  - `video_snapshots`: index `video_id`, `views`, `rate`, `crawl_id`, and `category` fields for fast queries.
  - `videos`: index `category`, `uploader`, `length_sec`.

Advantages of sharded cluster:
- Parallel read/writes and distributed storage — enables horizontal scale across multiple nodes.
- Reduced query latency for large datasets; if partitioned properly, Spark reads from the cluster in parallel via the `MongoSamplePartitioner` and other partitioning strategies.

Current repo support for parallel reads/writes to Mongo:
- `SparkConnector` config includes: `spark.mongodb.input.partitioner: MongoSamplePartitioner` and partition options (`partitionSizeMB`, `samplesPerPartition`) to support parallelized reads from a sharded or large collection.

### Performance benchmarks for ingestion/query in cluster deployments vs local

What is present in the repo and logs:
- The current repo and run logs use a local Spark cluster (`master: local[worker_cores]`) and local MongoDB. There are two primary modes:
  1. Local, sampled dataset (user-configured sample sizes — e.g., 50,000 edges) to keep UI interactive.
  2. Full-data mode, where Spark loads the entire collection (potentially slower)
- Example run: logs show "Detected 20 CPU cores, using 19 for Spark" followed by sample operations: loaded `edges`, sampled 50,000 edges out of 990,521 total, then computed in/out degrees and combined distribution (the analytics code also records timing inside the CLI script `Spark/Degree_distribution_Categorized_Statistics.py`).

What is missing: explicit cluster-scale benchmarks that run the application on a distributed Spark/Yarn/Kubernetes cluster or an Atlas sharded instance with measured ingest and query times. The repo includes local timing reports but not historical cluster-run logs. That means we cannot extract real multi-node performance numbers from the repo.

Recommendation for cluster benchmarks (how to measure and compare):
1. Baseline local testing:
   - Use `start.ps1` or run `streamlit run app/main.py` and measure the sampling queries and single-machine Spark operations. Log elapsed times for:
     - Loading collection (Spark read) — use `df.count()` or timing logs in diagnostic script (be mindful that `count()` triggers full evaluation).
     - Degree computations: time to compute `inDegree`, `outDegree`, combined `total_degree`.
     - PageRank/Label Propagation: time to run for a specified sample size.
     - Centrality (NetworkX): time to convert to pandas and run centrality metrics.

2. No-Cluster vs Cluster Performance Table (sample approach): create a small benchmark suite to measure:
   - Ingestion throughput: Insert `N` documents (videos, edges) and measure docs/sec; run on local single-node and on a sharded cluster.
   - Query read throughput: Time to load collection into Spark, and time for groupBy/joins/aggregations. Run both for sampled sets and full datasets. Compare runtime and throughput.
   - Graph algorithms: Run PageRank and Label propagation with the same sample size on a single node vs cluster; measure completion time and memory usage.

3. Example benchmark steps & metrics:
   - Metric: `wall-clock time` (s), `CPU utilization` (%), `memory usage` (GB), `shuffle bytes` (Spark stage metrics), `documents/sec` for ingestion.
   - Commands to run (example):
     - Dataset ingestion: Run the ingestion loader script with `time python ingestion_script.py --threads 8 --batch 1000` (the repo does not have a dedicated ingestion script, but you can write one using pymongo bulk writes).
     - Query time: Use SparkDriver (python or cli) to load collection and run tests:
       - `spark = get_spark_connector()` & `df = spark.load_collection_from_mongo('edges')` → measure time to `df.count()` with `time.time()`. Repeat with `sample()`.
       - For PageRank & degree: run the analytics functions and print timing durations (scripts already log elapsed times when run locally).

4. Expected comparison / general guidance:
   - For embarrassingly parallel operations (map/filter, per-document aggregations), a cluster with proper partitioning typically scales near-linearly: doubling the number of worker cores reduces time roughly by ~2x depending on the shuffle/IO overhead and networking.
   - For heavy shuffle operations (joins and graph algorithms that require global aggregation), speedups will be limited by network overhead and shuffle bottlenecks; use partitioning and broadcast small tables to minimize shuffles.
   - Running graph algorithms (PageRank) on very large graphs requires either GraphFrames/TGraphX on Spark cluster or graph-specific engines (GraphX, Pregel) — local runs are restricted by memory and the chosen sampling strategy.

Example observed local baseline from a dev run (seen in the run logs):
- Sample size: 50,000 edges sampled from 990,521 total
- Computation: Degrees and basic stats completed on local machine during the sample run (the runtime environment showed Spark session creation followed by degree computation; actual wall times are not preserved in the repo logs but the CLI script logs timings for individual steps when re-run).

If you need real numbers, please run the following benchmark plan and paste results:
1. Reproduce a baseline local run (single machine):
   - Spark: use existing `SparkConnector` settings
   - Run `DegreeAnalytics.run_full_analysis(sample_size=50000)` in the UI or CLI script and note the per-step logs/times printed.
2. Run on a small cluster (set `SparkSession.builder.master('spark://<cluster-master-host>:7077')` or submit to Yarn), enable `spark.mongodb.input.partitioner` and `partitionSizeMB`.
3. Repeat the same queries with full dataset (no sample) and record step times: `load_collection_from_mongo`, degree calculation, PageRank, community detection.

These runs will provide side-by-side data to answer how the cluster deployment compares to local runs.

### 3.b Running algorithms on a Hadoop/Spark Cluster

What the repo supports:
- `SparkConnector._create_session()` is set for local multi-core but includes many configs needed for a distributed cluster, including: memory configuration, broadcast settings, shuffle compression, adaptive query execution (AQE), and optimized partitioning for MongoDB reads.
- To run on a real cluster, change `.master(...)` to a cluster master (e.g., `spark://master:7077`, or Yarn `yarn`), and ensure the Spark driver/executor memory configs are adjusted for cluster nodes.
- `spark.jars.packages` includes `mongo-spark-connector` and `graphframes` to enable GraphFrames on Spark-3.5.x clusters.

What to check/adjust for cluster:
- Jar versions used by Spark on the cluster must match the Spark version; graphframes support must be installed (GraphFrames package) and matching Scala versions.
- Set `.master('yarn')` or `.master('spark://...')` and configure `spark.executor.instances`, `spark.executor.memory`, and `spark.executor.cores` based on cluster hardware.
- Use `MongoSamplePartitioner` or range-based partitioner in `SparkConnector` to scale up parallel read throughput:
  - `.config('spark.mongodb.input.partitioner', 'MongoSamplePartitioner')`
  - `partitionSizeMB` and `samplesPerPartition` control partitioning granularity.
- For write-heavy ingestion, use bulk writes (pymongo insertMany) with concurrency to maximize throughput.
- For PageRank and Label-propagation, GraphFrames executes in parallel across Spark workers — a cluster with sufficiently large memory and cores per worker reduces iteration time.

### 3.c Hardware used/cited in the repository

- The repo is configured and tested in a local multi-core environment. Example log from runs (observed in the provided runtime snippet in the workspace context):
  - Spark detected 20 CPU cores and configured Spark with `local[19]` (i.e., using 19 worker cores).
  - `spark.driver.memory` and `spark.executor.memory` are set to 4g in `SparkConnector` (local default; but `Degree_distribution_Categorized_Statistics.py` uses 3g for windows-specific runs).
  - The team notes a typical minimum local development environment should have Java 17 and 4GB+ memory per process; but this is the *development* baseline. The default in `SparkConnector` suggests a single machine with many cores (e.g., 16-20 cores).

Recommended cluster hardware for proper scalability testing (example reference configurations):
- Small cluster (dev): 3 nodes
  - Each node: 8 cores (or 16 logical cores), 32GB RAM, NVMe SSD 1TB
  - Driver: 8–16 cores, 32+ GB RAM
  - Use an HDFS or S3 for storage along with MongoDB sharded cluster for dataset storage.
- Medium cluster (production / real scale): 3–10 nodes
  - Each node: 16–32 cores, 64–256 GB RAM, NVMe RAID for shuffle and spill
  - Dedicated master nodes and spark worker nodes
- Additional considerations: network speed (10GbE or better), high IO throughput on data nodes, tunable spark executor memory, and disk IOPS are critical for shuffle-heavy operations like joins and PageRank.

---

## Summary — Findings & Recommendations ✅

- UI: Fully interactive Streamlit GUI with multiple pages, Plotly visualizations, network visualizations, and clickable links. Code is structured and modular. File references: `app/pages/*`, `app/components/*`.

- User queries: Degree, category, top-k, range queries, PageRank, community detection, centralities. The analytics (Spark-based) modules implement the queries and the UI calls them. See: `app/analytics/*`.

- Scalability: The code is ready to scale by modifying `SparkConnector` to connect to a real cluster (use `master` pointing to Yarn/Mesos/Spark Standalone or Kubernetes). It uses MongoDB partitioner config to parallelize reads. No explicit historical cluster-run benchmarks exist in the repo — only local logs and timing code that will log results when run.

- Hardware: The team tested locally on a machine with 20 logical cores and 4GB driver/executor memory in code; recommended cluster specs are provided for serious scaling tests.

- Next steps and recommendations:
  1. Add a small benchmark script (CLI) that runs selected analytics on: (a) sample (50k edges) and (b) full dataset; the script should log runtime per stage for ingestion, reads, degree computations, PageRank, and community detection. Use `Spark/Degree_distribution_Categorized_Statistics.py` as a starting point and modify it to produce a small `CSV` of benchmark measurements.
  2. For cluster deployment and to measure ingestion: replicate the dataset into a sharded MongoDB cluster and run the benchmark cases. Capture throughput (docs/sec), and query times. Use `MongoSamplePartitioner` in the Spark connector to test different `partitionSizeMB` and `samplesPerPartition` settings to find sweet spots.
  3. Tune Spark configs for cluster runs: tune `spark.executor.memory`, `spark.executor.cores`, `spark.executor.instances`, `spark.executor.memoryOverhead`, and `spark.sql.shuffle.partitions`. Add a `spark.yarn.executor.memoryOverhead` for Yarn if needed.
  4. Consider adding a `benchmarks/` folder and a script that automatically runs the predefined tests on sample vs full dataset across `n` cluster configurations (local single-node, 3-node cluster, 5-node cluster). Save the metrics to CSV and commit them to the repo.

---

If you’d like, I can now:
- Add a small benchmark script (e.g., `scripts/cluster_benchmarks.py`) that runs degree computation, PageRank, and top-k queries with timing logs for local and cluster run modes. (I can create it and include guidance for running it.)
- Add a short README snippet on how to run cluster benchmarks and recommended cluster settings.

Which of the above would you like me to implement next? (A benchmark script, docs update, or both?)
