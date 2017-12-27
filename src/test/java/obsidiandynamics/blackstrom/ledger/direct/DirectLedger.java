package obsidiandynamics.blackstrom.ledger.direct;

import java.util.*;

import obsidiandynamics.blackstrom.handler.*;
import obsidiandynamics.blackstrom.ledger.*;
import obsidiandynamics.blackstrom.model.*;

public final class DirectLedger implements Ledger {
  private final List<MessageHandler> handlers = new ArrayList<>();
  
  private final VotingContext context = new DefaultVotingContext(this);
  
  @Override
  public void attach(MessageHandler handler) {
    handlers.add(handler);
  }

  @Override
  public void append(Message message) throws Exception {
    for (int i = handlers.size(); --i >= 0; ) {
      handlers.get(i).onMessage(context, message);
    }
  }

  @Override
  public void dispose() {}
}
