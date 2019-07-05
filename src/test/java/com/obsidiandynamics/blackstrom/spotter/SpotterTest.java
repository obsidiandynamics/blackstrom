package com.obsidiandynamics.blackstrom.spotter;

import static com.obsidiandynamics.zerolog.LogLevel.*;
import static org.junit.Assert.*;

import org.assertj.core.api.*;
import org.junit.*;

import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

public final class SpotterTest {
  @Test
  public void testGetLots_empty() {
    final var spotter = new Spotter(new SpotterConfig());
    Assertions.assertThat(spotter.getLots()).isEmpty();
  }
  
  @Test
  public void testGetLots_sparse() {
    final var spotter = new Spotter(new SpotterConfig());
    spotter.addLot(1);
    spotter.addLot(3);
    Assertions.assertThat(spotter.getLots().values()).extracting(Lot::getShard).containsExactly(1, 3);
  }
  
  @Test
  public void testRemoveLot_nonExistent() {
    final var spotter = new Spotter(new SpotterConfig());
    spotter.addLot(4);
    assertNull(spotter.removeLot(3));
    assertNull(spotter.removeLot(5));
    assertNull(spotter.removeLot(50));
  }
  
  @Test
  public void testRemoveLot_existing() {
    final var spotter = new Spotter(new SpotterConfig());
    spotter.addLot(1);
    assertEquals(1, spotter.removeLot(1).getShard());
  }
  
  @Test
  public void testTryAdvance() {
    final var spotter = new Spotter(new SpotterConfig());
    assertTrue(spotter.tryAdvance(1, 10));
    assertTrue(spotter.tryAdvance(1, 11));
    assertFalse(spotter.tryAdvance(1, 9));
    assertEquals(11, spotter.getLots().get(1).getOffset());
  }
  
  @Test
  public void testPrintParkedLots_empty() {
    final var logTarget = new MockLogTarget();
    final var spotter = new Spotter(new SpotterConfig().withZlg(logTarget.logger()));
    spotter.printParkedLots();
    logTarget.entries().assertCount(0);
  }
  
  @Test
  public void testPrintParkedLots_notExpired() {
    final var logTarget = new MockLogTarget();
    final var spotter = new Spotter(new SpotterConfig()
                                    .withTimeout(60_000)
                                    .withZlg(logTarget.logger()));
    spotter.addLot(1);
    spotter.addLot(3);
    spotter.printParkedLots();
    logTarget.entries().assertCount(0);
  }
  
  @Test
  public void testPrintParkedLots_expiredButNotGraced() {
    final var logTarget = new MockLogTarget();
    final var spotter = new Spotter(new SpotterConfig()
                                    .withTimeout(0)
                                    .withGracePeriod(60_000)
                                    .withZlg(logTarget.logger()));
    spotter.addLot(1);
    spotter.addLot(3);
    Threads.sleep(5);
    spotter.printParkedLots();
    logTarget.entries().assertCount(0);
  }
  
  @Test
  public void testPrintParkedLots_expiredAndGraced() {
    final var logTarget = new MockLogTarget();
    final var spotter = new Spotter(new SpotterConfig()
                                    .withTimeout(0)
                                    .withGracePeriod(0)
                                    .withZlg(logTarget.logger()));
    spotter.addLot(1);
    spotter.addLot(3);
    assertTrue(spotter.tryAdvance(3, 10));
    Threads.sleep(5);
    spotter.printParkedLots();
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(INFO).containing("Parked: [1#?, 3#11]").assertCount(1);
    
    // verify that lots only get printed once, unless advanced
    logTarget.reset();
    Threads.sleep(5);
    assertFalse(spotter.tryAdvance(3, 10));
    spotter.printParkedLots();
    logTarget.entries().assertCount(0);
    
    // advance an offset and let it lapse; we should see it in the log
    logTarget.reset();
    assertTrue(spotter.tryAdvance(3, 11));
    Threads.sleep(5);
    spotter.printParkedLots();
    logTarget.entries().assertCount(1);
    logTarget.entries().forLevel(INFO).containing("Parked: [3#12]").assertCount(1);
  }
  
  @Test
  public void testPrintParkedLots_disabledLogger() {
    final var logTarget = new MockLogTarget(WARN);
    final var spotter = new Spotter(new SpotterConfig()
                                    .withTimeout(0)
                                    .withGracePeriod(0)
                                    .withZlg(logTarget.logger()));
    spotter.addLot(1);
    spotter.addLot(3);
    Threads.sleep(5);
    spotter.printParkedLots();
    logTarget.entries().assertCount(0);
  }
}
