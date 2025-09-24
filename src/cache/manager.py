"""Local caching system for historical data."""

import json
import pickle
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd
from dataclasses import dataclass, asdict
import threading
import time

@dataclass
class CacheEntry:
    """Cache entry metadata."""
    key: str
    data_type: str  # 'json', 'parquet', 'pickle'
    created_at: datetime
    expires_at: Optional[datetime]
    file_path: str
    size_bytes: int
    access_count: int
    last_accessed: datetime
    tags: List[str]


class LocalCacheManager:
    """Local file-based cache manager with memory index."""
    
    def __init__(self, cache_dir: str = "data/cache", max_size_mb: int = 500):
        """
        Initialize cache manager.
        
        Args:
            cache_dir: Directory to store cache files
            max_size_mb: Maximum cache size in MB
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_size_bytes = max_size_mb * 1024 * 1024
        
        # In-memory index of cache entries
        self.index: Dict[str, CacheEntry] = {}
        self.lock = threading.RLock()
        
        # Load existing cache index
        self._load_index()
        
        # Start cleanup thread
        self._start_cleanup_thread()
    
    def _generate_key(self, namespace: str, identifier: str, **kwargs) -> str:
        """Generate cache key from namespace and parameters."""
        key_data = f"{namespace}:{identifier}"
        if kwargs:
            # Sort kwargs for consistent key generation
            sorted_kwargs = sorted(kwargs.items())
            key_data += ":" + ":".join(f"{k}={v}" for k, v in sorted_kwargs)
        
        # Hash for consistent length
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_file_path(self, key: str, data_type: str) -> Path:
        """Get file path for cache entry."""
        extension = {
            'json': '.json',
            'parquet': '.parquet',
            'pickle': '.pkl'
        }.get(data_type, '.dat')
        
        return self.cache_dir / f"{key}{extension}"
    
    def _load_index(self):
        """Load cache index from disk."""
        index_file = self.cache_dir / "cache_index.json"
        if index_file.exists():
            try:
                with open(index_file, 'r') as f:
                    index_data = json.load(f)
                
                for key, entry_data in index_data.items():
                    # Convert datetime strings back to datetime objects
                    entry_data['created_at'] = datetime.fromisoformat(entry_data['created_at'])
                    entry_data['last_accessed'] = datetime.fromisoformat(entry_data['last_accessed'])
                    if entry_data['expires_at']:
                        entry_data['expires_at'] = datetime.fromisoformat(entry_data['expires_at'])
                    
                    self.index[key] = CacheEntry(**entry_data)
                
                print(f"Loaded cache index with {len(self.index)} entries")
            except Exception as e:
                print(f"Error loading cache index: {e}")
                self.index = {}
    
    def _save_index(self):
        """Save cache index to disk."""
        index_file = self.cache_dir / "cache_index.json"
        try:
            # Convert datetime objects to strings for JSON serialization
            serializable_index = {}
            for key, entry in self.index.items():
                entry_dict = asdict(entry)
                entry_dict['created_at'] = entry.created_at.isoformat()
                entry_dict['last_accessed'] = entry.last_accessed.isoformat()
                if entry.expires_at:
                    entry_dict['expires_at'] = entry.expires_at.isoformat()
                else:
                    entry_dict['expires_at'] = None
                serializable_index[key] = entry_dict
            
            with open(index_file, 'w') as f:
                json.dump(serializable_index, f, indent=2)
        except Exception as e:
            print(f"Error saving cache index: {e}")
    
    def put(self, namespace: str, identifier: str, data: Any, 
            ttl_hours: Optional[int] = None, tags: List[str] = None, **kwargs) -> str:
        """
        Store data in cache.
        
        Args:
            namespace: Cache namespace (e.g., 'google_trends', 'aviation')
            identifier: Unique identifier within namespace
            data: Data to cache
            ttl_hours: Time to live in hours
            tags: Tags for categorization
            **kwargs: Additional parameters for key generation
        
        Returns:
            Cache key
        """
        with self.lock:
            key = self._generate_key(namespace, identifier, **kwargs)
            tags = tags or []
            
            # Determine data type and serialize
            if isinstance(data, pd.DataFrame):
                data_type = 'parquet'
                file_path = self._get_file_path(key, data_type)
                data.to_parquet(file_path)
            elif isinstance(data, (dict, list)):
                data_type = 'json'
                file_path = self._get_file_path(key, data_type)
                with open(file_path, 'w') as f:
                    json.dump(data, f, default=str)
            else:
                data_type = 'pickle'
                file_path = self._get_file_path(key, data_type)
                with open(file_path, 'wb') as f:
                    pickle.dump(data, f)
            
            # Calculate expiration
            expires_at = None
            if ttl_hours:
                expires_at = datetime.utcnow() + timedelta(hours=ttl_hours)
            
            # Get file size
            size_bytes = file_path.stat().st_size
            
            # Create cache entry
            entry = CacheEntry(
                key=key,
                data_type=data_type,
                created_at=datetime.utcnow(),
                expires_at=expires_at,
                file_path=str(file_path),
                size_bytes=size_bytes,
                access_count=0,
                last_accessed=datetime.utcnow(),
                tags=tags
            )
            
            self.index[key] = entry
            self._save_index()
            
            # Check if we need to cleanup
            self._cleanup_if_needed()
            
            return key
    
    def get(self, namespace: str, identifier: str, **kwargs) -> Optional[Any]:
        """
        Retrieve data from cache.
        
        Args:
            namespace: Cache namespace
            identifier: Unique identifier
            **kwargs: Additional parameters for key generation
        
        Returns:
            Cached data or None if not found/expired
        """
        with self.lock:
            key = self._generate_key(namespace, identifier, **kwargs)
            
            if key not in self.index:
                return None
            
            entry = self.index[key]
            
            # Check if expired
            if entry.expires_at and datetime.utcnow() > entry.expires_at:
                self._remove_entry(key)
                return None
            
            # Check if file exists
            file_path = Path(entry.file_path)
            if not file_path.exists():
                self._remove_entry(key)
                return None
            
            # Load data
            try:
                if entry.data_type == 'parquet':
                    data = pd.read_parquet(file_path)
                elif entry.data_type == 'json':
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                else:  # pickle
                    with open(file_path, 'rb') as f:
                        data = pickle.load(f)
                
                # Update access statistics
                entry.access_count += 1
                entry.last_accessed = datetime.utcnow()
                self._save_index()
                
                return data
                
            except Exception as e:
                print(f"Error loading cached data {key}: {e}")
                self._remove_entry(key)
                return None
    
    def exists(self, namespace: str, identifier: str, **kwargs) -> bool:
        """Check if data exists in cache and is not expired."""
        with self.lock:
            key = self._generate_key(namespace, identifier, **kwargs)
            
            if key not in self.index:
                return False
            
            entry = self.index[key]
            
            # Check if expired
            if entry.expires_at and datetime.utcnow() > entry.expires_at:
                return False
            
            # Check if file exists
            return Path(entry.file_path).exists()
    
    def invalidate(self, namespace: str, identifier: str = None, tags: List[str] = None, **kwargs):
        """
        Invalidate cache entries.
        
        Args:
            namespace: Cache namespace
            identifier: Specific identifier (optional)
            tags: Invalidate by tags (optional)
            **kwargs: Additional parameters for key generation
        """
        with self.lock:
            keys_to_remove = []
            
            if identifier:
                # Invalidate specific entry
                key = self._generate_key(namespace, identifier, **kwargs)
                if key in self.index:
                    keys_to_remove.append(key)
            else:
                # Invalidate by namespace or tags
                for key, entry in self.index.items():
                    if key.startswith(namespace):
                        if tags:
                            # Check if entry has any of the specified tags
                            if any(tag in entry.tags for tag in tags):
                                keys_to_remove.append(key)
                        else:
                            keys_to_remove.append(key)
            
            for key in keys_to_remove:
                self._remove_entry(key)
            
            self._save_index()
    
    def _remove_entry(self, key: str):
        """Remove cache entry and file."""
        if key in self.index:
            entry = self.index[key]
            file_path = Path(entry.file_path)
            
            # Remove file
            if file_path.exists():
                try:
                    file_path.unlink()
                except Exception as e:
                    print(f"Error removing cache file {file_path}: {e}")
            
            # Remove from index
            del self.index[key]
    
    def _cleanup_if_needed(self):
        """Cleanup cache if size exceeds limit."""
        total_size = sum(entry.size_bytes for entry in self.index.values())
        
        if total_size > self.max_size_bytes:
            # Sort by last accessed (LRU)
            entries_by_access = sorted(
                self.index.items(),
                key=lambda x: x[1].last_accessed
            )
            
            # Remove oldest entries until under limit
            for key, entry in entries_by_access:
                self._remove_entry(key)
                total_size -= entry.size_bytes
                
                if total_size <= self.max_size_bytes * 0.8:  # Leave some headroom
                    break
    
    def _cleanup_expired(self):
        """Remove expired entries."""
        with self.lock:
            now = datetime.utcnow()
            expired_keys = []
            
            for key, entry in self.index.items():
                if entry.expires_at and now > entry.expires_at:
                    expired_keys.append(key)
            
            for key in expired_keys:
                self._remove_entry(key)
            
            if expired_keys:
                self._save_index()
                print(f"Cleaned up {len(expired_keys)} expired cache entries")
    
    def _start_cleanup_thread(self):
        """Start background cleanup thread."""
        def cleanup_worker():
            while True:
                time.sleep(3600)  # Run every hour
                try:
                    self._cleanup_expired()
                except Exception as e:
                    print(f"Error in cache cleanup: {e}")
        
        cleanup_thread = threading.Thread(target=cleanup_worker, daemon=True)
        cleanup_thread.start()
    
    def get_stats(self) -> Dict:
        """Get cache statistics."""
        with self.lock:
            total_size = sum(entry.size_bytes for entry in self.index.values())
            total_entries = len(self.index)
            
            # Group by namespace
            namespaces = {}
            for key, entry in self.index.items():
                namespace = key.split(':')[0] if ':' in key else 'unknown'
                if namespace not in namespaces:
                    namespaces[namespace] = {'count': 0, 'size': 0}
                namespaces[namespace]['count'] += 1
                namespaces[namespace]['size'] += entry.size_bytes
            
            # Group by data type
            data_types = {}
            for entry in self.index.values():
                if entry.data_type not in data_types:
                    data_types[entry.data_type] = {'count': 0, 'size': 0}
                data_types[entry.data_type]['count'] += 1
                data_types[entry.data_type]['size'] += entry.size_bytes
            
            return {
                'total_entries': total_entries,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'max_size_mb': round(self.max_size_bytes / (1024 * 1024), 2),
                'utilization_pct': round((total_size / self.max_size_bytes) * 100, 1),
                'namespaces': namespaces,
                'data_types': data_types,
                'cache_dir': str(self.cache_dir)
            }
    
    def clear_all(self):
        """Clear all cache entries."""
        with self.lock:
            keys_to_remove = list(self.index.keys())
            for key in keys_to_remove:
                self._remove_entry(key)
            self._save_index()
            print(f"Cleared {len(keys_to_remove)} cache entries")


# Global cache instance
_cache_manager = None

def get_cache_manager() -> LocalCacheManager:
    """Get global cache manager instance."""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = LocalCacheManager()
    return _cache_manager
