package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface Ledger extends Initable.Default, Disposable.Default {
  void attach(MessageHandler handler);
  
  void append(Message message) throws Exception;
}
