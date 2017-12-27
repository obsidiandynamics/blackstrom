package obsidiandynamics.blackstrom.model;

import java.util.*;

public final class Decision extends Message {
  private final Outcome outcome;
  private final Response[] responses;

  public Decision(Object messageId, Object ballotId, String source, Outcome outcome, Response[] responses) {
    super(messageId, ballotId, source);
    this.outcome = outcome;
    this.responses = responses;
  }
  
  public Outcome getOutcome() {
    return outcome;
  }
  
  public Response[] getResponses() {
    return responses;
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.DECISION;
  }

  @Override
  public String toString() {
    return "Decision [outcome=" + outcome + ", responses=" + Arrays.toString(responses) + ", " + baseToString() + "]";
  }
}
