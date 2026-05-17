package com.linkedin.metadata.queue;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Implements weighted fair queuing across priority bands. Distributes a total fetch limit
 * proportionally across bands, executing one query per band and redistributing unused capacity from
 * bands that have fewer messages than their allotment.
 */
public final class WeightedBandFetcher {

  private WeightedBandFetcher() {}

  /**
   * Functional interface for executing a single band-scoped fetch. Implementations should query
   * messages with {@code priority BETWEEN minPriority AND maxPriority} up to {@code limit} rows.
   */
  @FunctionalInterface
  public interface BandQuery<T> {
    List<T> fetch(int minPriority, int maxPriority, int limit) throws Exception;
  }

  /**
   * Fetches messages across all bands using weighted fair queuing. Each band gets a proportional
   * share of {@code totalLimit}. If a band returns fewer messages than its share, the unused
   * capacity is redistributed to subsequent bands (greedy fill).
   *
   * @param config the priority band configuration
   * @param totalLimit total number of messages to fetch across all bands
   * @param query the per-band query executor
   * @return aggregated results from all bands, ordered by band (highest priority first)
   */
  @Nonnull
  public static <T> List<T> fetch(
      @Nonnull PriorityBandConfig config, int totalLimit, @Nonnull BandQuery<T> query)
      throws Exception {
    if (totalLimit <= 0) {
      return List.of();
    }
    int[] limits = config.batchLimits(totalLimit);
    List<T> results = new ArrayList<>(totalLimit);
    int remaining = totalLimit;

    List<PriorityBand> bands = config.bands();
    for (int i = 0; i < bands.size() && remaining > 0; i++) {
      PriorityBand band = bands.get(i);
      int bandLimit = Math.min(limits[i], remaining);
      if (bandLimit <= 0) {
        continue;
      }
      List<T> fetched = query.fetch(band.minPriority(), band.maxPriority(), bandLimit);
      results.addAll(fetched);
      remaining -= fetched.size();
      int unused = bandLimit - fetched.size();
      if (unused > 0 && i + 1 < bands.size()) {
        limits[i + 1] += unused;
      }
    }
    return results;
  }
}
