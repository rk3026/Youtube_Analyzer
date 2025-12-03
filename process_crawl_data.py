#!/usr/bin/env python3
"""
Milestone 2 - YouTube Crawl Data Processing Pipeline
Unzips crawl folders, processes crawl data following log validation,
removes only malformed/duplicate data, and ingests into MongoDB.
"""

import json
import logging
import time
import zipfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Generator, Any
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from collections import defaultdict

# Configuration
RAW_DIR = Path("data/raw")
EXTRACTED_DIR = Path("data/extracted")
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "youtube_analytics"
BATCH_SIZE = 1000
YOUTUBE_START_DATE = datetime(2005, 2, 15)

# Collection names
COLLECTIONS = {
    'videos': 'videos',
    'snapshots': 'video_snapshots',
    'edges': 'edges',
    'crawls': 'crawls'
}

def setup_logging():
    """Configure logging for the pipeline"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('crawl_processing.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class CrawlDataProcessor:
    def __init__(self, mongo_uri: str, database_name: str):
        self.logger = setup_logging()
        self.client = MongoClient(mongo_uri)
        self.db = self.client[database_name]
        self.collections = {
            name: self.db[collection] 
            for name, collection in COLLECTIONS.items()
        }
        
        # Track duplicates and statistics
        self.seen_video_ids: Set[str] = set()
        self.crawl_stats = {}
        self.global_stats = {
            'total_processed': 0,
            'malformed_removed': 0,
            'duplicates_removed': 0,
            'valid_records': 0,
            'crawls_processed': 0
        }

    def extract_crawl_files(self):
        """Extract all zip files in raw directory"""
        self.logger.info("[EXTRACT] Extracting crawl files...")
        
        # Clean extraction directory
        if EXTRACTED_DIR.exists():
            shutil.rmtree(EXTRACTED_DIR)
        EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
        
        zip_files = list(RAW_DIR.glob("*.zip"))
        if not zip_files:
            self.logger.warning("No zip files found in raw directory")
            return []
        
        extracted_crawls = []
        
        for zip_file in sorted(zip_files):
            self.logger.info(f"  Extracting {zip_file.name}...")
            
            try:
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    # Extract to extraction directory
                    zip_ref.extractall(EXTRACTED_DIR)
                    
                    # The actual crawl directory might be nested
                    crawl_dir = EXTRACTED_DIR / zip_file.stem
                    if not crawl_dir.exists():
                        # Look for any directory that was created
                        possible_dirs = [d for d in EXTRACTED_DIR.iterdir() if d.is_dir()]
                        if possible_dirs:
                            crawl_dir = possible_dirs[0]
                    
                    extracted_crawls.append(crawl_dir)
                    
                    # Log extracted contents
                    contents = list(crawl_dir.rglob("*"))
                    self.logger.info(f"    Extracted {len(contents)} files to {crawl_dir}")
                    
            except Exception as e:
                self.logger.error(f"    Failed to extract {zip_file.name}: {e}")
        
        self.logger.info(f"[SUCCESS] Extracted {len(extracted_crawls)} crawl directories")
        return extracted_crawls

    def parse_crawl_log(self, crawl_dir: Path) -> Dict:
        """Parse crawl log file to get crawl metadata"""
        # First, always set the crawl_id from directory name
        crawl_info = {
            'crawl_id': crawl_dir.name,
            'parsed_at': datetime.now().isoformat()
        }
        
        log_files = list(crawl_dir.glob("*.txt"))
        log_file = None
        
        # Look for log file (usually has "log" in name or smallest txt file)
        for f in log_files:
            if "log" in f.name.lower():
                log_file = f
                break
        
        # If no explicit log file, try to find a small text file that might be a log
        if not log_file and log_files:
            # Sort by size and take smallest (logs are usually small)
            log_files_with_size = [(f, f.stat().st_size) for f in log_files]
            log_files_with_size.sort(key=lambda x: x[1])
            log_file = log_files_with_size[0][0]
        
        if not log_file:
            self.logger.warning(f"No log file found in {crawl_dir}, using directory name")
            return crawl_info
        
        self.logger.info(f"  [LOG] Parsing log: {log_file.name}")
        
        try:
            with log_file.open("r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            
            # Add log content to crawl info
            crawl_info['log_file'] = log_file.name
            crawl_info['raw_log'] = content[:1000]  # Truncate for storage
            
            # Extract timing information if available
            for line in content.split('\n'):
                line = line.strip().lower()
                if 'start:' in line:
                    crawl_info['start_time'] = line.replace('start:', '').strip()
                elif 'finish:' in line:
                    crawl_info['finish_time'] = line.replace('finish:', '').strip()
                elif 'total' in line:
                    # Try to extract totals
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            crawl_info['total_videos'] = int(parts[1])
                            crawl_info['total_time'] = int(parts[2])
                        except (ValueError, IndexError):
                            pass
            
            self.logger.info(f"    Crawl ID: {crawl_info['crawl_id']}")
            if 'start_time' in crawl_info:
                self.logger.info(f"    Start: {crawl_info['start_time']}")
            if 'finish_time' in crawl_info:
                self.logger.info(f"    Finish: {crawl_info['finish_time']}")
            
            return crawl_info
            
        except Exception as e:
            self.logger.error(f"Failed to parse log {log_file}: {e}")
            crawl_info['error'] = str(e)
            return crawl_info

    def safe_int(self, val, default=0):
        """Safely convert value to integer"""
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    def safe_float(self, val, default=0.0):
        """Safely convert value to float"""
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def validate_video_id(self, video_id: str) -> bool:
        """Validate YouTube video ID format (11 characters, alphanumeric + _ -)"""
        if not video_id or len(video_id) != 11:
            return False
        return all(c.isalnum() or c in '_-' for c in video_id)

    def normalize_category(self, cat: str) -> str:
        """Normalize category names"""
        if not cat or not isinstance(cat, str):
            return "unknown"
        return cat.strip().lower().replace("&", "and").replace(" ", "_")

    def age_to_date(self, age_days: int) -> str:
        """Convert age in days to upload date"""
        upload_date = YOUTUBE_START_DATE + timedelta(days=age_days)
        return upload_date.strftime("%Y-%m-%d")

    def is_malformed_record(self, parts: List[str]) -> Tuple[bool, str]:
        """Check if record is malformed (minimal validation)"""
        # Must have at least video_id, uploader, age, category, length, views, rate, ratings, comments
        if len(parts) < 9:
            return True, "insufficient_fields"
        
        # Video ID must be valid format
        video_id = parts[0].strip()
        if not self.validate_video_id(video_id):
            return True, "invalid_video_id"
        
        # Age must be numeric and reasonable
        age_days = self.safe_int(parts[2])
        if age_days < 0 or age_days > 10000:  # Very permissive bounds
            return True, "invalid_age"
        
        # Length must be positive
        length_sec = self.safe_int(parts[4])
        if length_sec <= 0:
            return True, "invalid_length"
        
        return False, ""

    def is_duplicate_record(self, video_id: str, crawl_id: str) -> bool:
        """Check if this is a duplicate record within the same crawl"""
        record_key = f"{video_id}_{crawl_id}"
        if record_key in self.seen_video_ids:
            return True
        self.seen_video_ids.add(record_key)
        
        # Clear memory periodically to prevent excessive memory usage
        if len(self.seen_video_ids) > 1000000:  # Clear after 1M records
            self.logger.info("    Clearing duplicate tracking cache...")
            self.seen_video_ids.clear()
        
        return False

    def parse_crawl_record(self, parts: List[str], crawl_id: str) -> Tuple[bool, Dict]:
        """Parse and clean a single crawl record"""
        # Check for malformed data
        is_malformed, reason = self.is_malformed_record(parts)
        if is_malformed:
            self.global_stats['malformed_removed'] += 1
            return False, {'error': f'malformed_{reason}'}
        
        video_id = parts[0].strip()
        
        # Check for duplicates
        if self.is_duplicate_record(video_id, crawl_id):
            self.global_stats['duplicates_removed'] += 1
            return False, {'error': 'duplicate'}
        
        # Clean and structure the data
        try:
            uploader = parts[1].strip() if parts[1] else "unknown"
            age_days = self.safe_int(parts[2])
            category = self.normalize_category(parts[3])
            length_sec = self.safe_int(parts[4])
            views = max(0, self.safe_int(parts[5]))  # Ensure non-negative
            rate = max(0.0, min(5.0, self.safe_float(parts[6])))  # Clamp 0-5
            ratings = max(0, self.safe_int(parts[7]))
            comments = max(0, self.safe_int(parts[8]))
            
            # Clean related video IDs
            related_ids = []
            for rid in parts[9:]:
                if rid and self.validate_video_id(rid.strip()):
                    related_ids.append(rid.strip())
            
            record = {
                'video_id': video_id,
                'uploader': uploader,
                'age_days': age_days,
                'upload_date': self.age_to_date(age_days),
                'category': category,
                'length_sec': length_sec,
                'views': views,
                'rate': rate,
                'ratings': ratings,
                'comments': comments,
                'related_ids': related_ids[:50],  # Limit to prevent oversized docs
                'crawl_id': crawl_id,
                'processed_at': datetime.now().isoformat()
            }
            
            self.global_stats['valid_records'] += 1
            return True, record
            
        except Exception as e:
            self.global_stats['malformed_removed'] += 1
            return False, {'error': f'parse_error: {str(e)}'}

    def create_documents(self, record: Dict) -> Tuple[Dict, Dict, List[Dict]]:
        """Transform record into MongoDB documents"""
        # Video document (static attributes)
        video_doc = {
            '_id': record['video_id'],
            'uploader': record['uploader'],
            'category': record['category'],
            'length_sec': record['length_sec'],
            'date_uploaded': record['upload_date'],
            'seen_in_crawls': [record['crawl_id']]
        }
        
        # Snapshot document (temporal metrics) - matches report schema
        snapshot_doc = {
            '_id': {"video_id": record['video_id'], "crawl_id": record['crawl_id']},
            'video_id': record['video_id'],
            'crawl_id': record['crawl_id'],
            'age_days': record['age_days'],
            'category': record['category'],  # Denormalized for query efficiency
            'length_sec': record['length_sec'],  # Denormalized for query efficiency
            'views': record['views'],
            'rate': record['rate'],
            'ratings': record['ratings'],
            'comments': record['comments']
        }
        
        # Edge documents (relationships) - matches report schema
        edge_docs = []
        for related_id in record['related_ids']:
            edge_docs.append({
                'crawl_id': record['crawl_id'],
                'src': record['video_id'],
                'dst': related_id
            })
        
        return video_doc, snapshot_doc, edge_docs

    def process_crawl_data_files(self, crawl_dir: Path, crawl_info: Dict) -> Generator[Tuple[Dict, Dict, List[Dict]], None, None]:
        """Process all data files in a crawl directory"""
        crawl_id = crawl_info['crawl_id']
        
        # Find data files (usually numbered: 0.txt, 1.txt, 2.txt, etc.)
        data_files = []
        all_txt_files = list(crawl_dir.glob("*.txt"))
        
        for f in all_txt_files:
            # Skip log files
            if f.name.lower() in ['log.txt', 'readme.txt']:
                continue
                
            # Check if it's a numbered data file or any other txt file that's not a log
            if f.stem.isdigit() or f.name.lower() != crawl_info.get('log_file', '').lower():
                data_files.append(f)
        
        if not data_files:
            self.logger.warning(f"No data files found in {crawl_dir}")
            self.logger.info(f"  Available files: {[f.name for f in all_txt_files]}")
            return
        
        data_files.sort()  # Process in order
        
        crawl_stats = {
            'total_lines': 0,
            'valid_records': 0,
            'malformed': 0,
            'duplicates': 0
        }
        
        for data_file in data_files:
            self.logger.info(f"    📄 Processing {data_file.name}...")
            
            file_count = 0
            file_valid = 0
            
            try:
                with data_file.open("r", encoding="utf-8", errors="ignore") as f:
                    for line_no, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        
                        parts = line.split('\t')
                        crawl_stats['total_lines'] += 1
                        file_count += 1
                        self.global_stats['total_processed'] += 1
                        
                        is_valid, record = self.parse_crawl_record(parts, crawl_id)
                        
                        if is_valid:
                            crawl_stats['valid_records'] += 1
                            file_valid += 1
                            
                            # Create MongoDB documents
                            video_doc, snapshot_doc, edge_docs = self.create_documents(record)
                            yield video_doc, snapshot_doc, edge_docs
                        else:
                            # Track specific error types
                            error_type = record.get('error', 'unknown')
                            if 'malformed' in error_type:
                                crawl_stats['malformed'] += 1
                            elif 'duplicate' in error_type:
                                crawl_stats['duplicates'] += 1
                        
                        # Progress indicator for large files
                        if file_count % 50000 == 0:
                            retention_rate = (file_valid / file_count * 100) if file_count > 0 else 0
                            self.logger.info(f"      Progress: {file_count:,} lines, {file_valid:,} valid ({retention_rate:.1f}%)")
            
            except Exception as e:
                self.logger.error(f"Error processing {data_file}: {e}")
                continue
            
            self.logger.info(f"      Completed {data_file.name}: {file_valid:,}/{file_count:,} valid ({file_valid/file_count*100:.1f}%)")
        
        # Store crawl statistics
        self.crawl_stats[crawl_id] = crawl_stats
        
        self.logger.info(f"  📊 Crawl {crawl_id} summary:")
        self.logger.info(f"    Total lines: {crawl_stats['total_lines']:,}")
        self.logger.info(f"    Valid records: {crawl_stats['valid_records']:,}")
        self.logger.info(f"    Malformed removed: {crawl_stats['malformed']:,}")
        self.logger.info(f"    Duplicates removed: {crawl_stats['duplicates']:,}")

    def create_indexes(self):
        """Create optimized indexes for all collections (safe to call multiple times)"""
        self.logger.info("[INDEX] Ensuring database indexes exist...")
        
        try:
            # Videos collection indexes
            self.collections['videos'].create_index("uploader", background=True)
            self.collections['videos'].create_index("category", background=True) 
            self.collections['videos'].create_index("date_uploaded", background=True)
            self.collections['videos'].create_index("length_sec", background=True)
            self.collections['videos'].create_index("seen_in_crawls", background=True)
            
            # Snapshots collection indexes
            self.collections['snapshots'].create_index("video_id", background=True)
            self.collections['snapshots'].create_index("crawl_id", background=True)
            self.collections['snapshots'].create_index([("video_id", 1), ("crawl_id", 1)], background=True)
            self.collections['snapshots'].create_index("views", background=True)
            self.collections['snapshots'].create_index("rate", background=True)
            self.collections['snapshots'].create_index([("category", 1), ("views", -1)], background=True)
            
            # Edges collection indexes
            self.collections['edges'].create_index("src", background=True)
            self.collections['edges'].create_index("dst", background=True)
            self.collections['edges'].create_index("crawl_id", background=True)
            self.collections['edges'].create_index([("src", 1), ("crawl_id", 1)], background=True)
            
            # Crawls collection indexes
            self.collections['crawls'].create_index("crawl_id", background=True)
            self.collections['crawls'].create_index("processed_at", background=True)
            
            self.logger.info("[SUCCESS] Database indexes ensured")
            
        except Exception as e:
            self.logger.error(f"Failed to create indexes: {e}")

    def batch_insert_documents(self, video_batch: List[UpdateOne], snapshot_batch: List[Dict], edge_batch: List[Dict]):
        """Perform batch insertions with error handling"""
        try:
            # Insert/update videos (handle seen_in_crawls merging)
            if video_batch:
                result = self.collections['videos'].bulk_write(video_batch, ordered=False)
                
            # Insert snapshots
            if snapshot_batch:
                result = self.collections['snapshots'].insert_many(snapshot_batch, ordered=False)
                
            # Insert edges
            if edge_batch:
                result = self.collections['edges'].insert_many(edge_batch, ordered=False)
                
        except BulkWriteError as e:
            # Log errors but continue processing
            self.logger.warning(f"Batch write errors: {len(e.details['writeErrors'])} errors")
            for error in e.details['writeErrors'][:5]:  # Log first 5 errors
                self.logger.warning(f"  Error: {error}")

    def get_processed_crawls(self) -> Set[str]:
        """Get list of already processed crawl IDs"""
        try:
            processed_crawls = set()
            crawls_cursor = self.collections['crawls'].find({}, {'_id': 1})
            for crawl in crawls_cursor:
                processed_crawls.add(crawl['_id'])
            return processed_crawls
        except Exception as e:
            self.logger.warning(f"Could not retrieve processed crawls: {e}")
            return set()

    def process_all_crawls(self, force_reprocess: bool = False):
        """Process crawl data with incremental processing support
        
        Args:
            force_reprocess: If True, clears all data and reprocesses everything.
                           If False (default), only processes new crawls.
        """
        start_time = time.time()
        
        # Extract crawl files
        crawl_dirs = self.extract_crawl_files()
        if not crawl_dirs:
            self.logger.error("No crawl directories to process")
            return 0
        
        if force_reprocess:
            # Clear existing data for full reprocessing
            self.logger.info("[CLEAN] Force reprocessing - clearing existing collections...")
            for collection in self.collections.values():
                collection.drop()
            new_crawl_dirs = crawl_dirs
        else:
            # Check for already processed crawls
            processed_crawls = self.get_processed_crawls()
            if processed_crawls:
                self.logger.info(f"[INCREMENTAL] Found {len(processed_crawls)} already processed crawls: {sorted(processed_crawls)}")
                
                # Filter out already processed crawls
                new_crawl_dirs = []
                for crawl_dir in crawl_dirs:
                    if crawl_dir.name not in processed_crawls:
                        new_crawl_dirs.append(crawl_dir)
                    else:
                        self.logger.info(f"[SKIP] Crawl {crawl_dir.name} already processed")
                
                if not new_crawl_dirs:
                    self.logger.info("[COMPLETE] No new crawls to process - all crawls already in database")
                    return 0
                
                self.logger.info(f"[NEW] Processing {len(new_crawl_dirs)} new crawls: {[d.name for d in new_crawl_dirs]}")
            else:
                self.logger.info("[INITIAL] No existing crawls found - processing all crawls")
                new_crawl_dirs = crawl_dirs
        
        # Ensure indexes exist (safe to call multiple times)
        self.create_indexes()
        
        # Initialize batch containers
        video_batch = []
        snapshot_batch = []
        edge_batch = []
        
        # Process each new crawl directory
        for crawl_dir in new_crawl_dirs:
            self.logger.info(f"[PROCESS] Processing crawl: {crawl_dir.name}")
            
            # Parse crawl metadata
            crawl_info = self.parse_crawl_log(crawl_dir)
            
            # Store crawl metadata (matches report schema)
            crawl_doc = {
                '_id': crawl_info['crawl_id'],
                'date': datetime.now().isoformat(),
                'notes': f"Crawl processing from {crawl_info.get('start_time', 'unknown')} to {crawl_info.get('finish_time', 'unknown')}",
                'log_info': crawl_info,
                'processed_at': datetime.now().isoformat()
            }
            self.collections['crawls'].insert_one(crawl_doc)
            
            # Process crawl data files
            batch_count = 0
            for video_doc, snapshot_doc, edge_docs in self.process_crawl_data_files(crawl_dir, crawl_info):
                # Add to batches
                video_upsert = UpdateOne(
                    {'_id': video_doc['_id']},
                    {
                        '$set': {k: v for k, v in video_doc.items() if k != 'seen_in_crawls'},
                        '$addToSet': {'seen_in_crawls': {'$each': video_doc['seen_in_crawls']}}
                    },
                    upsert=True
                )
                video_batch.append(video_upsert)
                snapshot_batch.append(snapshot_doc)
                edge_batch.extend(edge_docs)
                
                # Process in batches
                if len(snapshot_batch) >= BATCH_SIZE:
                    self.batch_insert_documents(video_batch, snapshot_batch, edge_batch)
                    video_batch = []
                    snapshot_batch = []
                    edge_batch = []
                    batch_count += 1
                    
                    if batch_count % 10 == 0:
                        self.logger.info(f"      Processed {batch_count * BATCH_SIZE:,} records...")
            
            self.global_stats['crawls_processed'] += 1
            self.logger.info(f"[SUCCESS] Completed crawl: {crawl_dir.name}")
        
        # Process remaining batches
        if video_batch or snapshot_batch or edge_batch:
            self.batch_insert_documents(video_batch, snapshot_batch, edge_batch)
        
        # Clean up extracted files
        if EXTRACTED_DIR.exists():
            shutil.rmtree(EXTRACTED_DIR)
            self.logger.info("[CLEANUP] Cleaned up extracted files")
        
        return time.time() - start_time

def main():
    """Main processing pipeline with incremental processing support"""
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process YouTube crawl data into MongoDB')
    parser.add_argument('--force-reprocess', action='store_true', 
                       help='Force reprocessing of all crawls (clears existing data)')
    args = parser.parse_args()
    
    processor = CrawlDataProcessor(MONGO_URI, DATABASE_NAME)
    
    try:
        # Test MongoDB connection
        processor.client.admin.command('ping')
        processor.logger.info(f"[CONNECT] Connected to MongoDB: {MONGO_URI}")
        
        # Process crawl data (incremental by default, full if --force-reprocess)
        if args.force_reprocess:
            processor.logger.info("[MODE] Force reprocessing mode - will clear all existing data")
        else:
            processor.logger.info("[MODE] Incremental processing mode - will skip already processed crawls")
        
        processing_time = processor.process_all_crawls(force_reprocess=args.force_reprocess)
        
        if processing_time > 0:
            # Log completion stats
            processor.logger.info(f"[COMPLETE] Crawl data processing completed successfully!")
            processor.logger.info(f"Processing time: {processing_time:.1f} seconds")
            processor.logger.info(f"Records processed: {processor.global_stats['total_processed']:,}")
            processor.logger.info(f"Valid records: {processor.global_stats['valid_records']:,}")
            processor.logger.info(f"Crawls processed: {processor.global_stats['crawls_processed']}")
            processor.logger.info("Run validate_crawl_processing.py for detailed validation and reporting")
        
    except Exception as e:
        processor.logger.error(f"Processing failed: {e}")
        raise
    finally:
        processor.client.close()

if __name__ == "__main__":
    main()