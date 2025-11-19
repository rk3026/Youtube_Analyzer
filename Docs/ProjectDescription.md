1. Project: Youtube Analyzer
Related Industry: Social Media
Data: http://netsg.cs.sfu.ca/youtubedata/
Suggested Problem: Implement a Youtube data analyzer supported by
NoSQL database, Hadoop MapReduce, and Spark GraphX/GraphFrame.
The analyzer provides basic data analytics functions to Youtube media
datasets. The analyzer provides following functions for users:
• Network aggregation: efficiently report the following statistics of
Youtube video network:
- Degree distribution (including in-degree and out-degree); average
degree, maximum and minimum degree
- Categorized statistics: frequency of videos partitioned by a search
condition: categorization, size of videos, view count, etc.
• Search:
- Top k queries: find top k categories in which the most number of
videos are uploaded; top k rated videos; top k most popular videos.
- Range queries: find all videos in categories X with duration within a
range [t1, t2]; find all videos with size in range [x,y].
- User identification in recommendation patterns: find all occurrence
of a specified subgraph pattern connecting users and videos with
specified search condition.
• Influence analysis:
- Use PageRank algorithms over the Youtube network to compute the
scores efficiently. Intuitively, a video with high PageRank score
means that the video is related to many videos in the graph, thus has
a high influence.
- Effectively find top k most influence videos in Youtube network.
Check the properties of these videos (# of views, # edges,
category...). What can we find out? Present your findings.
Note:
- For the purpose of the project, data from the first few crawls would
suffice, provided it qualifies as “big data”.
- Where applicable, develop effective optimization techniques to speed
up the algorithm you used, including indexing, compression, or
summarization.