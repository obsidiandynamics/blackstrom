package com.obsidiandynamics.blackstrom.ledger;

import com.obsidiandynamics.blackstrom.*;
import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.manifold.*;
import com.obsidiandynamics.blackstrom.model.*;

public interface Ledger extends Disposable.Nop {
  static AppendCallback SYS_ERR_APPEND_CALLBACK = AppendCallback.errorLoggingAppendCallback(System.err);
  
  static AppendCallback getDefaultAppendCallback() {
    return SYS_ERR_APPEND_CALLBACK;
  }
  
  /**
   *  Invoked by the {@link Manifold} to initialise the ledger.
   */
  default void init() {}
  
  /**
   *  Attaches the given handler to the ledger.
   *  
   *  @param handler The ledger to attach.
   *  @return An optional unique ID of the handler. The ID is guaranteed to be non-{@code null} for a
   *          group-based handler. Otherwise the ID <em>may</em> be {@code null}.
   */
  Object attach(MessageHandler handler);
  
  /**
   *  Appends a message to the ledger using a custom append callback.
   *  
   *  @param message The message to append.
   *  @param callback Invoked when the message has either been successfully appended, or if the append failed.
   */
  void append(Message message, AppendCallback callback);
  
  /**
   *  Determines if the given shard is currently assigned to the handler.
   *  
   *  @param handlerId The handler ID (may be {@code null} for non-group handlers).
   *  @param shard The shard.
   *  @return True if the handler is the assignee of the shard, or if the handler is not a part of a group.
   */
  boolean isAssigned(Object handlerId, int shard);
  
  /**
   *  Appends a message to the ledger, using the default append callback.
   *  
   *  @param message The message to append.
   */
  default void append(Message message) {
    append(message, getDefaultAppendCallback());
  }
  
  /**
   *  Confirms that the given message has been processed, advancing the consumer's state. <p>
   *  
   *  This method can only be invoked if the handler is subscribed within a group. Otherwise, an
   *  {@link IllegalStateException} will be thrown.
   *  
   *  @param handlerId The handler ID.
   *  @param messageId The message ID.
   */
  default void confirm(Object handlerId, MessageId messageId) {}
}
