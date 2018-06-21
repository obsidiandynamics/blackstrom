package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class QueryResponse extends FluentMessage<QueryResponse> {
  private final Object reply;
  
  public QueryResponse(String xid, Object reply) {
    this(xid, NOW, reply);
  }

  public QueryResponse(String xid, long timestamp, Object reply) {
    super(xid, timestamp);
    this.reply = reply;
  }
  
  public <T> T getObjective() {
    return Classes.cast(reply);
  }
  
  @Override
  public MessageType getMessageType() {
    return MessageType.QUERY_RESPONSE;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(reply)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof QueryResponse) {
      final QueryResponse that = (QueryResponse) obj;
      return new EqualsBuilder()
          .appendSuper(baseEquals(that))
          .append(reply, that.reply)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return QueryResponse.class.getSimpleName() + " [" + baseToString() + ", reply=" + reply + "]";
  }
  
  @Override
  public QueryResponse shallowCopy() {
    return copyMutableFields(this, new QueryResponse(getBallotId(), getTimestamp(), reply));
  }
}
