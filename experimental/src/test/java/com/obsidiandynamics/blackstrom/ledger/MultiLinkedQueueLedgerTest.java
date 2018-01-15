package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.blackstrom.util.*;

public final class MultiLinkedQueueLedgerTest extends AbstractLedgerTest {
  @Override
  protected Timesert getWait() {
    return Wait.SHORT;
  }
  
  @Override
  protected Ledger createLedgerImpl() {
    return new MultiLinkedQueueLedger();
  }
}
