Non-relational schema:
4 collections in MongoDB

videos: stores static attributes for each video
{
  "_id": "video_id", // from the data’s video id
  "uploader": "…",
  "category": "…",
  "length_sec": 213,
  “date_uploaded”: ISODate(“2007-01-01T00:00:00Z”), // calculated from age
  “seen_in_crawls”: [“0222”, “0301”, “0302”]
}

video_snapshots
{
  "_id": { "video_id": "dQw4w9WgXcQ", "crawl_id": "0222" },
  "video_id": "dQw4w9WgXcQ",
  "crawl_id": "0222",
  "age_days": 1234,                    // as defined: days since YouTube establishment
  "category": "Music",
  "length_sec": 213,                   // integer
  "views": 1234567,
  "rate": 4.62,                        // float
  "ratings": 9876,
  "comments": 5432
}

edges:
{
  "_id": ObjectId(),
  "crawl_id": "0222",
  "src": "dQw4w9WgXcQ",           // the video containing the "related IDs" list
  "dst": "oHg5SJYRHA0"           // one related video id
}

crawls: handy for per-crawl reporting/filters
{
  "_id": "0222",
  "date": ISODate("2007-03-01T00:00:00Z"),
  "notes": "crawled to fifth depth, but did not finish”,
  "total_videos": 155513,
  "duration_sec": 93352
}
