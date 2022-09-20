package io.github.uharaqo.consistenthashing;

import java.util.Collection;
import java.util.function.UnaryOperator;
import javax.annotation.Nullable;

public interface ConsistentHashSelector<K, V> {

  /**
   * Select the instance for this key.
   *
   * @param key Key to select an instance
   * @return an instance or null iff no instance is kept in this list.
   */
  @Nullable
  V select(@Nullable K key);

  Collection<V> values();

  /**
   * Create a new selector with the given instances.
   *
   * @param instancesFactory Provides instances to be created with
   * @return A new selector. There's no change to the original object.
   */
  ConsistentHashSelector<K, V> cloneWith(UnaryOperator<Collection<V>> instancesFactory);

  /**
   * Create a new selector
   *
   * @see #create(Collection, HashFunction, int)
   */
  static <K, V> ConsistentHashSelector<K, V> create(
      Collection<V> instances, HashFunction<K, V> hashFunction) {
    return create(instances, hashFunction, 256);
  }

  /**
   * Create a new selector
   *
   * @param <K> key type
   * @param <V> value type
   * @param instances instances to be selected
   * @param hashFunction provides a hash for keys and values
   * @param vNodeReplicationFactor the number of replicas to be kept for each instance
   * @return a new selector
   */
  static <K, V> ConsistentHashSelector<K, V> create(
      Collection<V> instances, HashFunction<K, V> hashFunction, int vNodeReplicationFactor) {
    if (instances == null
        || instances.isEmpty()
        || hashFunction == null
        || vNodeReplicationFactor <= 0) {
      return new EmptyConsistentHashSelector<>(vNodeReplicationFactor, hashFunction);
    } else {
      return new DefaultConsistentHashSelector<>(instances, vNodeReplicationFactor, hashFunction);
    }
  }
}
