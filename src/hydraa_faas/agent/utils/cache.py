"""
Deployment cache for FaaS agent to improve performance and reduce redundant operations
"""

import hashlib
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import threading


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    data: Dict[str, Any]
    created_at: datetime
    last_accessed: datetime
    access_count: int = 0


class DeploymentCache:
    """Thread-safe deployment cache with TTL and LRU eviction"""

    def __init__(self, max_size: int = 1000, ttl_hours: int = 24):
        self.max_size = max_size
        self.ttl = timedelta(hours=ttl_hours)
        self._cache: Dict[str, CacheEntry] = {}
        self._status_cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def generate_key(self,
                     name: str,
                     handler: str,
                     runtime: str,
                     code_content: Optional[bytes] = None,
                     image_name: Optional[str] = None,
                     dynamic_config: Optional[str] = None) -> str:
        """Generate cache key from deployment parameters including dynamic config."""
        config_data = {
            'name': name,
            'handler': handler,
            'runtime': runtime,
            'dynamic_config': dynamic_config
        }

        # Create a hash from either the code content or the image name
        if code_content:
            content_hash = hashlib.sha256(code_content).hexdigest()[:16]
        elif image_name:
            content_hash = hashlib.sha256(image_name.encode()).hexdigest()[:16]
        else:
            content_hash = "no_content"

        config_str = json.dumps(config_data, sort_keys=True)
        config_hash = hashlib.sha256(config_str.encode()).hexdigest()[:16]

        return f"{name}_{config_hash}_{content_hash}"

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached deployment result"""
        with self._lock:
            entry = self._cache.get(key)
            if not entry:
                self._misses += 1
                return None

            if datetime.utcnow() - entry.created_at > self.ttl:
                del self._cache[key]
                self._misses += 1
                return None

            entry.last_accessed = datetime.utcnow()
            entry.access_count += 1
            self._hits += 1
            return entry.data

    def set(self, key: str, data: Dict[str, Any]) -> None:
        """Cache deployment result"""
        with self._lock:
            if len(self._cache) >= self.max_size:
                self._evict_lru()
            self._clean_expired()
            self._cache[key] = CacheEntry(
                data=data,
                created_at=datetime.utcnow(),
                last_accessed=datetime.utcnow()
            )

    def set_status(self, function_name: str, status: str, error: Optional[str] = None) -> None:
        """Set function deployment status"""
        with self._lock:
            now = datetime.utcnow()
            if function_name not in self._status_cache:
                self._status_cache[function_name] = {'created_at': now}

            self._status_cache[function_name].update({
                'status': status, 'updated_at': now, 'error': error
            })

    def get_status(self, function_name: str) -> Optional[Dict[str, Any]]:
        """Get function deployment status"""
        with self._lock:
            return self._status_cache.get(function_name)

    def remove_function(self, function_name: str) -> None:
        """Remove all cache entries for a function"""
        with self._lock:
            self._status_cache.pop(function_name, None)
            keys_to_remove = [k for k in self._cache if k.startswith(f"{function_name}_")]
            for key in keys_to_remove:
                del self._cache[key]

    def size(self) -> int:
        """Get current cache size"""
        with self._lock:
            return len(self._cache)

    def hit_rate(self) -> float:
        """Get cache hit rate"""
        with self._lock:
            total = self._hits + self._misses
            return self._hits / total if total > 0 else 0.0

    def stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        with self._lock:
            return {
                'size': len(self._cache), 'max_size': self.max_size,
                'hit_rate': self.hit_rate(), 'total_hits': self._hits,
                'total_misses': self._misses, 'status_entries': len(self._status_cache)
            }

    def clear(self) -> None:
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
            self._status_cache.clear()
            self._hits = 0
            self._misses = 0

    def _evict_lru(self) -> None:
        """Evict least recently used entries"""
        if not self._cache: return
        lru_key = min(self._cache, key=lambda k: self._cache[k].last_accessed)
        del self._cache[lru_key]

    def _clean_expired(self) -> None:
        """Remove expired entries"""
        now = datetime.utcnow()
        expired_keys = [k for k, v in self._cache.items() if now - v.created_at > self.ttl]
        for key in expired_keys:
            del self._cache[key]