package jp.co.cyberagent.valor.sdk.metadata;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SchemaRepositoryCache<K, V> implements Map<K, V> {

  private Long ttlSec;

  private ConcurrentHashMap<K, CacheEntry<V>> cache;

  public SchemaRepositoryCache(Long ttlSec) {
    this.ttlSec = ttlSec;
    this.cache = new ConcurrentHashMap<>();
  }

  @Override
  public V put(K key, V value) {
    CacheEntry<V> cacheEntry = new CacheEntry<>(value, ttlSec);
    clearInvalid();
    this.cache.put(key, cacheEntry);
    return value;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> otherMap) {
    for (Entry<? extends K, ? extends V> entry : otherMap.entrySet()) {
      this.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public V get(Object key) {
    if (!cache.containsKey(key)) {
      return null;
    }

    synchronized (cache) {
      CacheEntry<V> cacheEntry = cache.get((K)key);
      if (cacheEntry.isValid()) {
        return cacheEntry.getValue();
      } else {
        cache.remove(key);
        return null;
      }
    }
  }

  public Set<K> keySet() {
    clearInvalid();
    return cache.keySet();
  }

  @Override
  public Collection<V> values() {
    clearInvalid();
    return this.cache.values().stream().map(c -> c.getValue())
        .collect(Collectors.toList());
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    Set<Map.Entry<K, V>> entries = new HashSet<>();
    clearInvalid();
    for (Map.Entry<K, CacheEntry<V>> entry : this.cache.entrySet()) {
      entries.add(new SimpleEntry<>(entry.getKey(), entry.getValue().getValue()));
    }
    return entries;
  }

  @Override
  public boolean isEmpty() {
    return this.cache.isEmpty();
  }

  @Override
  public void clear() {
    this.cache.clear();
  }

  @Override
  public V remove(Object key) {
    return this.cache.remove(key).getValue();
  }

  @Override
  public boolean containsKey(Object key) {
    return this.cache.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return this.cache.containsValue(value);
  }

  @Override
  public int size() {
    return this.cache.size();
  }

  public boolean isAllEntryValid() {
    return cache.values().stream().anyMatch(c -> !c.isValid());
  }

  private void clearInvalid() {
    synchronized (cache) {
      List<K> expiredKeys = cache.entrySet().stream()
          .filter(e -> e.getValue().isValid())
          .map(e -> e.getKey())
          .collect(Collectors.toList());
      for (K expiredKey: expiredKeys) {
        cache.remove(expiredKey);
      }
    }
  }

  class CacheEntry<T> {

    private T value;
    private Long ttlMs;
    private Long createdTime;

    public CacheEntry(T value, Long ttlSec) {
      this.value = value;
      if (ttlSec != null) {
        this.ttlMs = ttlSec * 1_000L;
        this.createdTime = new Date().getTime();
      } else {
        this.ttlMs = null;
        this.createdTime = null;
      }
    }

    public boolean isValid() {
      return ttlMs == null || (new Date().getTime()) - createdTime <= ttlMs;
    }

    public T getValue() {
      return value;
    }
  }
}
