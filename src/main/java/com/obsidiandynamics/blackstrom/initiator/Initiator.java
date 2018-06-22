package com.obsidiandynamics.blackstrom.initiator;

import com.obsidiandynamics.blackstrom.factor.*;

public interface Initiator extends Factor, QueryResponseProcessor, CommandResponseProcessor, OutcomeProcessor {}
