package com.obsidiandynamics.blackstrom.ledger;

import java.io.*;
import java.util.function.*;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface Ledger extends Disposable.Default {
  static AppendCallback SYS_ERR_APPEND_CALLBACK = exceptionLoggingAppendCallback(System.err);
  
  static AppendCallback exceptionLoggingAppendCallback(PrintStream stream) {
    return appendExceptionHandler(x -> x.printStackTrace(stream));
  }
  
  static AppendCallback appendExceptionHandler(Consumer<Exception> exceptionHandler) {
    return (id, x) -> { 
      if (x != null) exceptionHandler.accept(x);
    };
  }
  
  default void init() {}
  
  void attach(MessageHandler handler);
  
  void append(Message message, AppendCallback callback);
  
  default void append(Message message) {
    append(message, SYS_ERR_APPEND_CALLBACK);
  }
  
  void confirm(Object handlerId, MessageId messageId);
}
