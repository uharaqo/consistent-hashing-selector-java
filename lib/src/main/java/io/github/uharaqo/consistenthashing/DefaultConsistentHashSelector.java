package io.github.uharaqo.consistenthashing;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

final class DefaultConsistentHashSelector<K, V> implements ConsistentHashSelector<K, V> {

  private final int vNodeReplicationFactor;
  private final HashFunction<K, V> hashFunction;
  private final TreeMap<Integer, V> ring = new TreeMap<>();

  DefaultConsistentHashSelector(
      Iterable<V> instances, int vNodeReplicationFactor, HashFunction<K, V> hashFunction) {
    this.vNodeReplicationFactor = vNodeReplicationFactor;
    this.hashFunction = hashFunction;

    for (V instance : instances) {
      for (int i = 0; i < vNodeReplicationFactor; i++) {
        addVNode(instance, i);
      }
    }
  }

  private void addVNode(V instance, int vNodeIndex) {
    ring.put(hashFunction.hashValue(instance, vNodeIndex), instance);
  }

  @Override
  @Nullable
  public V select(@Nullable K key) {
    if (key == null) {
      return ring.firstEntry().getValue();
    }

    int hash = hashFunction.hashKey(key);

    Entry<Integer, V> nextInstance = ring.ceilingEntry(hash);
    return nextInstance != null ? nextInstance.getValue() : ring.firstEntry().getValue();
  }

  @Override
  public Collection<V> values() {
    return ring.values();
  }

  @Override
  public ConsistentHashSelector<K, V> cloneWith(UnaryOperator<Collection<V>> instancesFactory) {
    return ConsistentHashSelector.create(
        instancesFactory.apply(ring.values()), hashFunction, vNodeReplicationFactor);
  }
}
