package com.linkedin.metadata.queue;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

public class WeightedBandFetcherTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final PriorityBandConfig DEFAULT_CONFIG =
      PriorityBandConfig.parse(
          MAPPER,
          "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]");

  @Test
  public void allMessagesInOneBand_redistributesCapacity() throws Exception {
    List<String> result =
        WeightedBandFetcher.fetch(
            DEFAULT_CONFIG,
            10,
            (minPri, maxPri, limit) -> {
              if (minPri == 4 && maxPri == 6) {
                List<String> msgs = new ArrayList<>();
                for (int i = 0; i < Math.min(limit, 5); i++) {
                  msgs.add("msg-" + i);
                }
                return msgs;
              }
              return List.of();
            });
    assertEquals(result.size(), 5);
  }

  @Test
  public void evenSpreadAcrossBands() throws Exception {
    List<String> result =
        WeightedBandFetcher.fetch(
            DEFAULT_CONFIG,
            10,
            (minPri, maxPri, limit) -> {
              List<String> msgs = new ArrayList<>();
              for (int i = 0; i < limit; i++) {
                msgs.add("band[" + minPri + "-" + maxPri + "]-" + i);
              }
              return msgs;
            });
    assertEquals(result.size(), 10);
  }

  @Test
  public void bandReturnsFewerThanLimit_redistributed() throws Exception {
    List<String> result =
        WeightedBandFetcher.fetch(
            DEFAULT_CONFIG,
            10,
            (minPri, maxPri, limit) -> {
              if (minPri == 0 && maxPri == 3) {
                return List.of("high-1");
              }
              List<String> msgs = new ArrayList<>();
              for (int i = 0; i < Math.min(limit, 20); i++) {
                msgs.add("band[" + minPri + "-" + maxPri + "]-" + i);
              }
              return msgs;
            });
    assertEquals(result.size(), 10);
    assertEquals(result.get(0), "high-1");
  }

  @Test
  public void emptyQueue() throws Exception {
    List<String> result =
        WeightedBandFetcher.fetch(DEFAULT_CONFIG, 10, (minPri, maxPri, limit) -> List.of());
    assertTrue(result.isEmpty());
  }

  @Test
  public void zeroLimit() throws Exception {
    List<String> result =
        WeightedBandFetcher.fetch(
            DEFAULT_CONFIG,
            0,
            (minPri, maxPri, limit) -> {
              fail("Should not be called with zero limit");
              return List.of();
            });
    assertTrue(result.isEmpty());
  }

  @Test
  public void singleBandConfig() throws Exception {
    PriorityBandConfig singleBand =
        PriorityBandConfig.parse(MAPPER, "[{\"range\":[0,9],\"weight\":100}]");
    List<String> result =
        WeightedBandFetcher.fetch(
            singleBand,
            5,
            (minPri, maxPri, limit) -> {
              assertEquals(minPri, 0);
              assertEquals(maxPri, 9);
              assertEquals(limit, 5);
              return List.of("a", "b", "c");
            });
    assertEquals(result.size(), 3);
  }
}
