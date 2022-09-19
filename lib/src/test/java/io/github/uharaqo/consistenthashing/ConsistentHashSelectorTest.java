package io.github.uharaqo.consistenthashing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.apache.commons.codec.digest.MurmurHash3;
import org.junit.jupiter.api.Test;

class ConsistentHashSelectorTest {

  @Test
  void test() {
    List<String> nodes =
        IntStream.range(0, 8).boxed().map(i -> "node-" + i).collect(Collectors.toList());
    int n = 100_000;
    int vNodeReplicationFactor = 256;

    ConsistentHashSelector<Integer, String> original =
        ConsistentHashSelector.create(nodes, new CommonsHashFunction());

    assertThat(original.values().size()).isEqualTo(nodes.size() * vNodeReplicationFactor);
    assertThat(original.select(null)).isNotNull();

    val v1 = testAndGroupByInput(n, original);

    ConsistentHashSelector<Integer, String> oneRemoved =
        original.cloneWith(
            c -> c.stream().filter(s -> !"node-0".equals(s)).collect(Collectors.toList()));

    val v2 = testAndGroupByInput(n, oneRemoved);

    int diff1 = diff(v1, v2);
    val rebalancedKeyPercent = ((double) diff1) / n;

    assertThat(rebalancedKeyPercent).isLessThan(1.0 / (double) nodes.size());

    ConsistentHashSelector<Integer, String> empty = original.cloneWith(c -> emptyList());
    assertThat(empty.select(1)).isNull();
    assertThat(empty.values()).isEmpty();

    ConsistentHashSelector<Integer, String> recreated = empty.cloneWith(c -> nodes);
    val v3 = testAndGroupByInput(n, recreated);
    val diff2 = diff(v1, v3);
    assertThat(diff2).isEqualTo(0);
  }

  private Map<Integer, String> testAndGroupByInput(
      int n, ConsistentHashSelector<Integer, String> selector) {
    return IntStream.range(0, n)
        .boxed()
        .map(i -> Map.entry(i, selector.select(i)))
        .reduce(
            new HashMap<>(),
            (m, e) -> {
              m.put(e.getKey(), e.getValue());
              return m;
            },
            (m1, m2) -> m1);
  }

  private static int diff(Map<Integer, String> v1, Map<Integer, String> v2) {
    AtomicInteger i = new AtomicInteger();
    v1.forEach(
        (k, v) -> {
          if (v2.get(k) == null || !v2.get(k).equals(v)) {
            i.incrementAndGet();
          }
        });
    return i.get();
  }

  private static class CommonsHashFunction implements HashFunction<Integer, String> {

    @Override
    public int hashKey(Integer key) {
      return MurmurHash3.hash32x86(BigInteger.valueOf(key).toByteArray());
    }

    @Override
    public int hashValue(String instance, int nodeIndex) {
      return MurmurHash3.hash32x86((instance + nodeIndex).getBytes(UTF_8));
    }
  }
}
