package jp.co.cyberagent.valor.spi.util;

import java.util.List;

/**
 * Iterate a multidimension array
 */
public class HeteroDigitCounter {
  private int targetSize;
  private int[] limits;
  private int[] counter;
  private boolean terminated;

  private HeteroDigitCounter(int targetSize, int[] limits) {
    this.targetSize = targetSize;
    this.limits = limits;
    this.terminated = false;
  }

  /**
   * buildTuple counter
   *
   * @param target 　A List of Lists expressing a multi dimension array
   * @return a generated counter if the target does not contain null elements. otherwise null.
   */
  public static <T> HeteroDigitCounter buildHeteroDigitCounter(List<? extends List<T>> target) {
    int targetSize = target.size();
    int[] limits = new int[targetSize];
    for (int i = 0; i < limits.length; i++) {
      List<?> element = target.get(i);
      limits[i] = element.size();
    }
    return new HeteroDigitCounter(targetSize, limits);
  }

  public int[] next() {
    if (terminated) {
      return null;
    }
    if (this.counter == null) {
      return initCounter();
    }
    boolean incremented = false;
    for (int i = 0; i < targetSize; i++) {
      if (this.counter[i] == this.limits[i] - 1) {
        this.counter[i] = 0;
      } else {
        this.counter[i]++;
        incremented = true;
        break;
      }
    }
    if (incremented) {
      return this.counter;
    } else {
      this.terminated = true;
      return null;
    }
  }

  private int[] initCounter() {
    if (limits.length == 0) {
      return null;
    }
    this.counter = new int[limits.length];
    for (int i = 0; i < targetSize; i++) {
      if (this.limits[i] != 0) {
        this.counter[i] = 0;
      } else {
        return null;
      }
    }
    return this.counter;
  }

  public int[] getCounter() {
    return counter;
  }
}
