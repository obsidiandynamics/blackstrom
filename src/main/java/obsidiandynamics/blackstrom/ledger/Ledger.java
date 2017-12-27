package obsidiandynamics.blackstrom.ledger;

import obsidiandynamics.blackstrom.*;
import obsidiandynamics.blackstrom.handler.*;
import obsidiandynamics.blackstrom.model.*;

public interface Ledger extends Disposable {
  void attach(MessageHandler handler);
  
  void append(Message message) throws Exception;
}
