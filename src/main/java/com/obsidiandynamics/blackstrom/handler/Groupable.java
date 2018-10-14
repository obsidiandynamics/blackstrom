package com.obsidiandynamics.blackstrom.handler;

@FunctionalInterface
public interface Groupable {
  String getGroupId();
  
  interface ClassGroup extends Groupable {
    @Override
    default String getGroupId() {
      return getClass().getSimpleName();
    }
  }
  
  interface NullGroup extends Groupable {
    @Override
    default String getGroupId() {
      return null;
    }
  }
}
