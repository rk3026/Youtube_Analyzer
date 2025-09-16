# YouTube Analyzer

A data analytics platform for large-scale YouTube video network datasets. The project leverages **NoSQL (MongoDB)**, **Hadoop MapReduce**, and **Apache Spark (GraphX/GraphFrames)** to store, process, and analyze YouTube data, uncovering insights into trends, influence, and user interaction patterns.

---

## 📌 Project Overview
YouTube is one of the world’s most socially and commercially influential platforms. This project builds an **end-to-end YouTube Analyzer** that:

- Efficiently stores and processes large-scale YouTube video datasets.
- Provides analytics on **network structure, influence, and trends**.
- Supports **top-k queries, range queries, and influence analysis** using modern big data tools.

Dataset: ["Statistics and Social Network of YouTube Videos"](http://netsg.cs.sfu.ca/youtubedata/) by Xu Cheng, Cameron Dale, and Jiangchuan Liu.

---

## 🎯 Features

### 1. **Network Aggregation**
- Degree distribution (in-degree, out-degree, avg, min, max).
- Categorized statistics (by video category, size, views, etc.).

### 2. **Search & Queries**
- **Top-k queries**:
  - Most popular videos
  - Highest-rated videos
  - Categories with the most uploads
- **Range queries**:
  - Videos by duration `[t1, t2]`
  - Videos by size `[x, y]`

### 3. **Influence Analysis**
- PageRank on the YouTube video graph to find **top-k most influential videos**.
- Analysis of influential video properties (views, edges, categories, etc.).

### 4. **Pattern & User Analysis**
- Subgraph/motif queries (e.g., user-video relationships in recommendation paths).

---

## 🛠️ Tech Stack

- **Database**: MongoDB (Atlas & local)
- **Processing Engines**: Hadoop MapReduce, Apache Spark
- **Graph Analytics**: Spark GraphX / GraphFrames
- **Optional**: Firebase (cloud storage for collections), Flask/FastAPI (REST API)

---

## 📂 Data Models

- **Document Store** (MongoDB): stores video metadata as documents with attributes (uploader, category, views, etc.).
- **Graph Model** (Spark GraphFrames): videos as nodes, related videos as edges for network analysis.
- **Key-Value Model** (alternative): video_id → metadata JSON, user_id → associated videos.
- **Wide-Column (HBase/Cassandra)** (optional): optimized for time-series and fast scans.

---

## 🚀 Goals

- Build a **CLI and/or dashboard** to trigger analytics jobs.
- Provide **visualizations** for degree distribution, PageRank, and query results.
- If time permits:
  - Develop a **REST API** (Flask/FastAPI) with a minimal dashboard.
  - Integrate with cloud resources for scalability.

---

## 👨‍💻 Team

| Name           | Role                                 | Email                | GitHub      |
| -------------- | ------------------------------------ | -------------------- | ----------- |
| Ross Kugler    | Data Pipeline & API Lead, Hadoop MapReduce Researcher, Communication Liaison | ross.kugler@wsu.edu  | [rk3026](https://github.com/rk3026)      |
| Huy (Harry) Ky | Database Manager, MongoDB Researcher            | giahuy.ky@wsu.edu    | [Harry908](https://github.com/Harry908)    |
| Ben Bordon     | Analytics & Algorithms Lead, Spark GraphX/GraphFrames Researcher, Documentation | b.bordon@wsu.edu | [wizkid0101](https://github.com/wizkid0101)  |

---

## 📊 Example Use Cases

- **Trend Discovery**: Identify common traits among top-k influential videos (e.g., all from a specific category).
- **Recommendation Insights**: Explore user patterns within the related-video network.
- **Marketing & Research**: Analyze popularity trends for targeted campaigns.
- **Content Moderation**: Detect clusters of related videos for community safety.

---

## 🔧 Setup Instructions

1. Clone the repository:
   ```bash
   git clone https://github.com/<your-username>/youtube-analyzer.git
   cd youtube-analyzer
   ```

2. Set up environment (Python 3.x + dependencies):
   ```bash
   pip install -r requirements.txt
   ```

3. Configure MongoDB connection (local or Atlas).

4. Run Hadoop/Spark jobs:
   ```bash
   spark-submit jobs/network_analysis.py
   ```

---

## 📜 License

This project is for **educational purposes** (database systems & big data analytics).
See [LICENSE](LICENSE) for details.
