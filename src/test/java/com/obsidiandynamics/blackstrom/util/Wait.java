package com.obsidiandynamics.blackstrom.util;

import com.obsidiandynamics.await.*;

public interface Wait {
  Timesert SHORT = Timesert.wait(10_000);
  Timesert MEDIUM = Timesert.wait(30_000);
  Timesert LONG = Timesert.wait(120_000);
}
