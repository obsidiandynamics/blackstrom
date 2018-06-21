package com.obsidiandynamics.blackstrom.model;

public enum MessageType {
  QUERY,
  QUERY_RESPONSE,
  
  COMMAND,
  COMMAND_RESPONSE,
  
  NOTICE,
  
  PROPOSAL,
  VOTE,
  OUTCOME,
  
  $UNKNOWN
}
