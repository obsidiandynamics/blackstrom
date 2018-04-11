package com.obsidiandynamics.blackstrom.retention;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;
import com.obsidiandynamics.flow.*;

@FunctionalInterface
public interface Retention {
  Confirmation begin(MessageContext context, Message message);
}
