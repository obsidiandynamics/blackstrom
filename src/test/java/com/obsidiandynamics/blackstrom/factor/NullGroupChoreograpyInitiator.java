package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.initiator.*;

public interface NullGroupChoreograpyInitiator 
extends Initiator, QueryResponseProcessor.Nop, CommandResponseProcessor.Nop, Groupable.NullGroup {}