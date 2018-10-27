package com.obsidiandynamics.blackstrom.ledger;

import java.util.concurrent.*;

import com.obsidiandynamics.blackstrom.model.*;

/**
 *  An {@link AppendCallback} implementation in the form of a {@link CompletableFuture}.
 */
public final class FutureAppendCallback extends CompletableFuture<MessageId> implements AppendCallback {
  @Override
  public void onAppend(MessageId messageId, Throwable error) {
    if (error != null) {
      completeExceptionally(error);
    } else {
      complete(messageId);
    }
  }
}
