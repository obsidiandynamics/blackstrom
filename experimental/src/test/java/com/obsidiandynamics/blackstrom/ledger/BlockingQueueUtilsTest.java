package com.obsidiandynamics.blackstrom.ledger;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.*;

import org.junit.*;

import com.obsidiandynamics.blackstrom.model.*;

public final class BlockingQueueUtilsTest {
  @Test
  public void test() {
    final BlockingQueue<Message> q = new LinkedBlockingQueue<>();
    final AppendCallback callback = mock(AppendCallback.class);
    
    BlockingQueueUtils.put(q, new UnknownMessage(0L).withMessageId("id"), callback);
    verify(callback).onAppend(eq("id"), isNull());
    
    Thread.currentThread().interrupt();
    BlockingQueueUtils.put(q, new UnknownMessage(0L), callback);
    verify(callback).onAppend(isNull(), isA(InterruptedException.class));
  }
}
