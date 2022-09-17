package com.github.uharaqo.consistenthashing;

public interface HashFunction<K, V> {

  int hashKey(K key);

  int hashValue(V instance, int nodeIndex);
}
