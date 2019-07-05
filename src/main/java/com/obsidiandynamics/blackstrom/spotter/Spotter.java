package com.obsidiandynamics.blackstrom.spotter;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import com.obsidiandynamics.zerolog.*;

public final class Spotter {
  private static final int INITIAL_LOTS = 1;
  
  private final long timeout;
  
  private final long gracePeriod;
  
  private final Zlg zlg;
  
  private Lot[] lots = new Lot[INITIAL_LOTS];
  
  public Spotter(SpotterConfig config) {
    mustExist(config, "Config cannot be null").validate();
    timeout = config.getTimeout();
    gracePeriod = config.getGracePeriod();
    zlg = config.getZlg();
  }
  
  private Lot getOrCreateLot(int shard) {
    if (shard >= lots.length) {
      final var newLots = new Lot[(shard + 1) * 2];
      System.arraycopy(lots, 0, newLots, 0, lots.length);
      lots = newLots;
    }
    
    if (lots[shard] == null) {
      lots[shard] = new Lot(shard);
    }
    
    return lots[shard];
  }
  
  public Lot addLot(int shard) {
    return getOrCreateLot(shard);
  }
  
  public Lot removeLot(int shard) {
    if (shard < lots.length) {
      final var existing = lots[shard];
      lots[shard] = null;
      return existing;
    } else {
      return null;
    }
  }
  
  public boolean tryAdvance(int shard, long offset) {
    return getOrCreateLot(shard).tryAdvance(offset);
  }
  
  public List<Lot> getLapsedLots() {
    List<Lot> lapsedLots = null;
    
    final var timeoutThreshold = System.currentTimeMillis() - timeout;
    final var graceThreshold = timeoutThreshold - gracePeriod;
    var willPrint = false;
    for (var lot : lots) {
      if (lot != null && ! lot.isLogPrinted() && lot.getLastAdvancedTime() < timeoutThreshold) {
        if (lapsedLots == null) lapsedLots = new ArrayList<>();
        lapsedLots.add(lot);
        
        if (lot.getLastAdvancedTime() < graceThreshold) {
          willPrint = true;
        }
      }
    }
    
    return willPrint ? lapsedLots : Collections.emptyList();
  }
  
  public void printParkedLots() {
    if (zlg.isEnabled(LogLevel.INFO)) {
      final var lapsedLots = getLapsedLots();
      
      if (! lapsedLots.isEmpty()) {
        final var parkedLots = new ArrayList<String>(lapsedLots.size());
        for (var lapsedLot : lapsedLots) {
          final var offset = lapsedLot.getOffset();
          parkedLots.add(lapsedLot.getShard() + "#" + (offset == -1 ? "?" : String.valueOf(offset + 1)));
          lapsedLot.setLogPrinted();
        }
        zlg.i("Parked: %s", z -> z.arg(parkedLots));
      }
    }
  }
  
  public Map<Integer, Lot> getLots() {
    final var lotsMap = new LinkedHashMap<Integer, Lot>(lots.length, 1f);
    for (var lot : lots) {
      if (lot != null) {
        lotsMap.put(lot.getShard(), lot);
      }
    }
    return Collections.unmodifiableMap(lotsMap);
  }
}
