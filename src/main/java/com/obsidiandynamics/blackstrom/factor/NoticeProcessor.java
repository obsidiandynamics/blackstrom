package com.obsidiandynamics.blackstrom.factor;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

@FunctionalInterface
public interface NoticeProcessor extends ElementalProcessor {
  void onNotice(MessageContext context, Notice notice);
  
  interface Nop extends NoticeProcessor {
    @Override 
    default void onNotice(MessageContext context, Notice notice) {}
  }
  
  interface BeginAndConfirm extends NoticeProcessor {
    @Override 
    default void onNotice(MessageContext context, Notice notice) {
      context.beginAndConfirm(notice);
    }
  }
}
