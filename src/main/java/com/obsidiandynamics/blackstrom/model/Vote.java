package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

public final class Vote extends FluentMessage<Vote> {
  private final Response response;
  
  public Vote(String xid, Response response) {
    this(xid, 0, response);
  }

  public Vote(String xid, long timestamp, Response response) {
    super(xid, timestamp);
    this.response = response;
  }
  
  public Response getResponse() {
    return response;
  }

  @Override
  public MessageType getMessageType() {
    return MessageType.VOTE;
  }
  
  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .appendSuper(baseHashCode())
        .append(response)
        .toHashCode();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Vote) {
      final Vote that = (Vote) obj;
      return new EqualsBuilder()
          .appendSuper(baseEquals(that))
          .append(response, that.response)
          .isEquals();
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Vote.class.getSimpleName() + " [" + baseToString() + ", response=" + response + "]";
  }
  
  @Override
  public Vote shallowCopy() {
    return copyMutableFields(this, new Vote(getBallotId(), getTimestamp(), response));
  }
}
