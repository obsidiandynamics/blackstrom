package com.obsidiandynamics.blackstrom.retention;

import com.obsidiandynamics.blackstrom.flow.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface Retention {
  Confirmation begin(MessageContext context, Message message);
}