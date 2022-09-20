package io.github.uharaqo.consistenthashing;

import java.util.Collection;
import java.util.Collections;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

final class EmptyConsistentHashSelector<K, V> implements ConsistentHashSelector<K, V> {

  private final int vNodeReplicationFactor;
  private final HashFunction<K, V> hashFunction;

  EmptyConsistentHashSelector(int vNodeReplicationFactor, HashFunction<K, V> hashFunction) {
    this.vNodeReplicationFactor = vNodeReplicationFactor;
    this.hashFunction = hashFunction;
  }

  @Nullable
  @Override
  public V select(@Nullable K key) {
    return null;
  }

  @Override
  public Collection<V> values() {
    return Collections.emptyList();
  }

  @Override
  public ConsistentHashSelector<K, V> cloneWith(UnaryOperator<Collection<V>> instancesFactory) {
    return ConsistentHashSelector.create(
        instancesFactory.apply(Collections.emptyList()), hashFunction, vNodeReplicationFactor);
  }
}
