package com.obsidiandynamics.blackstrom.factor;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class BlenderTest {
  @Test
  public void testAllMessageTypes() {
    final Blender blender = mock(Blender.class, Answers.CALLS_REAL_METHODS);
    final MessageContext context = mock(MessageContext.class);
    final Query query = new Query("X0", null, 1_000);
    final QueryResponse queryResponse = new QueryResponse("X0", null);
    final Command command = new Command("X0", null, 1_000);
    final CommandResponse commandResponse = new CommandResponse("X0", null);
    final Notice notice = new Notice("X0", null);
    final Proposal proposal = new Proposal("X0", new String[0], null, 1_000);
    final Vote vote = new Vote("X0", null);
    final Outcome outcome = new Outcome("X0", Resolution.COMMIT, null, new Response[0], null);
    
    blender.onQuery(context, query);
    blender.onQueryResponse(context, queryResponse);
    blender.onCommand(context, command);
    blender.onCommandResponse(context, commandResponse);
    blender.onNotice(context, notice);
    blender.onProposal(context, proposal);
    blender.onVote(context, vote);
    blender.onOutcome(context, outcome);
    verify(blender).onMessage(eq(context), eq(query));
    verify(blender).onMessage(eq(context), eq(queryResponse));
    verify(blender).onMessage(eq(context), eq(command));
    verify(blender).onMessage(eq(context), eq(commandResponse));
    verify(blender).onMessage(eq(context), eq(notice));
    verify(blender).onMessage(eq(context), eq(proposal));
    verify(blender).onMessage(eq(context), eq(vote));
    verify(blender).onMessage(eq(context), eq(outcome));
    verifyNoMoreInteractions(context);
  }
  
  @Test
  public void testConstructor() {
    new Blender() {
      @Override
      public void onMessage(MessageContext context, Message message) {}
    };
  }
}
