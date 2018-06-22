package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

import com.obsidiandynamics.func.*;

public final class QueryResponse extends FluentMessage<QueryResponse> {
  private final Object result;
  
  public QueryResponse(String xid, Object result) {
    this(xid, NOW, result);
  }

  public QueryResponse(String xid, long timestamp, Object result) {
    super(xid, timestamp);
    this.result = result;
  }
  
  public <T> T getResult() {
    return Classes.cast(result);
  }
  
  @Override
  public MessageType getMessageType() {
    return MessageType.QUERY_RESPONSE;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(result)
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
          .append(result, that.result)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return QueryResponse.class.getSimpleName() + " [" + baseToString() + ", result=" + result + "]";
  }
  
  @Override
  public QueryResponse shallowCopy() {
    return copyMutableFields(this, new QueryResponse(getXid(), getTimestamp(), result));
  }
}
