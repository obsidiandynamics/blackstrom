package com.obsidiandynamics.blackstrom.model;

import org.apache.commons.lang3.builder.*;

public final class Vote extends FluentMessage<Vote> {
  private final Response response;
  
  public Vote(Object ballotId, Response response) {
    this(ballotId, 0, response);
  }

  public Vote(Object ballotId, long timestamp, Response response) {
    super(ballotId, timestamp);
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
        .appendSuper(super.hashCode())
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
          .appendSuper(super.equals(obj))
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
}
