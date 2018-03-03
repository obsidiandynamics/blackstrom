package com.obsidiandynamics.blackstrom.retention;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class NopRetention implements Retention {
  private static final NopRetention instance = new NopRetention();
  
  public static NopRetention getInstance() {
    return instance;
  }
  
  private NopRetention() {}
  
  @Override
  public Confirmation begin(MessageContext context, Message message) {
    return NopConfirmation.getInstance();
  }
}
