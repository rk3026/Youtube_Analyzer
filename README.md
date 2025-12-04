# YouTube Analyzer

A data analytics platform for large-scale YouTube video network datasets. The project leverages **NoSQL (MongoDB)**, **Hadoop MapReduce**, and **Apache Spark (GraphX/GraphFrames)** to store, process, and analyze YouTube data, uncovering insights into trends, influence, and user interaction patterns.

---

## Project Overview
YouTube is one of the world’s most socially and commercially influential platforms. This project builds an **end-to-end YouTube Analyzer** that:

- Efficiently stores and processes large-scale YouTube video datasets.
- Provides analytics on **network structure, influence, and trends**.
- Supports **top-k queries, range queries, and influence analysis** using modern big data tools.

Dataset: ["Statistics and Social Network of YouTube Videos"](http://netsg.cs.sfu.ca/youtubedata/) by Xu Cheng, Cameron Dale, and Jiangchuan Liu.

---

## Architecture
<img width="341" height="365" alt="image" src="https://github.com/user-attachments/assets/6e023f90-71e8-4128-915b-babb2df703f6" />

Our architecture involves four main components – a parser, MongoDB database, Apache Spark algorithm set, and a Streamlit GUI app:
- Parsing Algorithm
  - Parses the raw crawl .txt files from the initial dataset
  - Cleans the data of missing/invalid values
  - Inserts into MongoDB
- MongoDB instance
  - 4 collections
    - crawls
    - edges
    - video_snapshots
    - videos
- Apache Spark
  - connects to MongoDB via the official connector
  - algorithms written in Python with PySpark
  - runs in standalone mode, requires at least 1 worker with 4 GB of memory
- Python GUI application using Streamlit framework
  - Connects to both MongoDB and Spark
  - Loads precomputed algorithm results from MongoDB
  - Can dynamically run Spark algorithms based on user queries

---

## App Preview
<img width="1919" height="860" alt="image" src="https://github.com/user-attachments/assets/31588bca-deae-4838-ac1a-5f47e804335b" />
<img width="1919" height="847" alt="image" src="https://github.com/user-attachments/assets/dc1454d3-f477-4826-a625-22fe1f7d9f4e" />
<img width="1919" height="855" alt="image" src="https://github.com/user-attachments/assets/ada0f660-4697-42ea-b2be-e9e1c88a3099" />
<img width="1919" height="847" alt="image" src="https://github.com/user-attachments/assets/fcd3f33d-0277-4611-aa05-377a26e2bd4c" />

---

## Features

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

## Tech Stack

- **Database**: MongoDB
- **Processing Engine**: Apache Spark
- **Graph Analytics**: Spark GraphFrames

---

## Data Model

- **Document Store** (MongoDB): stores video metadata as documents with attributes (uploader, category, views, etc.).


---

## Team

| Name           | Role                                 | Email                | GitHub      |
| -------------- | ------------------------------------ | -------------------- | ----------- |
| Ross Kugler    | Data Pipeline & API Lead, Hadoop MapReduce Researcher, Communication Liaison | ross.kugler@wsu.edu  | [rk3026](https://github.com/rk3026)      |
| Huy (Harry) Ky | Database Manager, MongoDB Researcher            | giahuy.ky@wsu.edu    | [Harry908](https://github.com/Harry908)    |
| Ben Bordon     | Analytics & Algorithms Lead, Spark GraphX/GraphFrames Researcher, Documentation | b.bordon@wsu.edu | [wizkid0101](https://github.com/wizkid0101)  |

