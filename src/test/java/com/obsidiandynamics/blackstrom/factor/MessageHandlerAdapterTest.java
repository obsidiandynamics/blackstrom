package com.obsidiandynamics.blackstrom.factor;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.blackstrom.handler.*;
import com.obsidiandynamics.blackstrom.model.*;

public final class MessageHandlerAdapterTest {
  interface ProposalFactor extends Factor, ProposalProcessor, Groupable.NullGroup {};
  
  interface VoteFactor extends Factor, VoteProcessor, Groupable.NullGroup {};
  
  interface OutcomeFactor extends Factor, OutcomeProcessor, Groupable.NullGroup {};
  
  interface QueryFactor extends Factor, QueryProcessor, Groupable.NullGroup {};
  
  interface QueryResponseFactor extends Factor, QueryResponseProcessor, Groupable.NullGroup {};
  
  interface CommandFactor extends Factor, CommandProcessor, Groupable.NullGroup {};
  
  interface CommandResponseFactor extends Factor, CommandResponseProcessor, Groupable.NullGroup {};
  
  interface NoticeFactor extends Factor, NoticeProcessor, Groupable.NullGroup {};
  
  @Test
  public void testProposalAndGroup() {
    final ProposalFactor factor = mock(ProposalFactor.class, Answers.CALLS_REAL_METHODS);
    when(factor.getGroupId()).thenReturn("test-proposal-group");
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onProposal(isNotNull(), isA(Proposal.class));
    assertEquals("test-proposal-group", adapter.getGroupId());
  }

  @Test
  public void testVoteAndNullGroup() {
    final VoteFactor factor = mock(VoteFactor.class, Answers.CALLS_REAL_METHODS);
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onVote(isNotNull(), isA(Vote.class));
    assertNull(adapter.getGroupId());
  }

  @Test
  public void testOutcome() {
    final OutcomeFactor factor = mock(OutcomeFactor.class, Answers.CALLS_REAL_METHODS);
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onOutcome(isNotNull(), isA(Outcome.class));
  }

  @Test
  public void testQuery() {
    final QueryFactor factor = mock(QueryFactor.class, Answers.CALLS_REAL_METHODS);
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onQuery(isNotNull(), isA(Query.class));
  }

  @Test
  public void testQueryResponse() {
    final QueryResponseFactor factor = mock(QueryResponseFactor.class, Answers.CALLS_REAL_METHODS);
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onQueryResponse(isNotNull(), isA(QueryResponse.class));
  }

  @Test
  public void testCommand() {
    final CommandFactor factor = mock(CommandFactor.class, Answers.CALLS_REAL_METHODS);
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onCommand(isNotNull(), isA(Command.class));
  }

  @Test
  public void testCommandResponse() {
    final CommandResponseFactor factor = mock(CommandResponseFactor.class, Answers.CALLS_REAL_METHODS);
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onCommandResponse(isNotNull(), isA(CommandResponse.class));
  }
  
  @Test
  public void testNotice() {
    final NoticeFactor factor = mock(NoticeFactor.class, Answers.CALLS_REAL_METHODS);
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(factor);
    callAll(adapter);
    verify(factor).onNotice(isNotNull(), isA(Notice.class));
  }
  
  @Test(expected=UnsupportedOperationException.class)
  public void testUnsupported() {
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new NullGroupFactor() {});
    adapter.onMessage(null, new UnknownMessage("X0"));
  }
  
  @Test(expected=NullPointerException.class)
  public void testNull() {
    final MessageHandlerAdapter adapter = new MessageHandlerAdapter(new NullGroupFactor() {});
    adapter.onMessage(null, new Message("X0", Message.NOW) {
      @Override 
      public MessageType getMessageType() {
        return null;
      }

      @Override
      public Message shallowCopy() {
        throw new UnsupportedOperationException();
      }
    });
  }
  
  private static void callAll(MessageHandlerAdapter adapter) {
    final MessageContext context = mock(MessageContext.class);
    adapter.onMessage(context, newQuery());
    adapter.onMessage(context, newQueryResponse());
    adapter.onMessage(context, newCommand());
    adapter.onMessage(context, newCommandResponse());
    adapter.onMessage(context, newNotice());
    adapter.onMessage(context, newProposal());
    adapter.onMessage(context, newVote());
    adapter.onMessage(context, newOutcome());
  }
  
  private static Query newQuery() {
    return new Query("X0", null, 0);
  }
  
  private static QueryResponse newQueryResponse() {
    return new QueryResponse("X0", null);
  }
  
  private static Command newCommand() {
    return new Command("X0", null, 0);
  }
  
  private static CommandResponse newCommandResponse() {
    return new CommandResponse("X0", null);
  }
  
  private static Notice newNotice() {
    return new Notice("X0", null);
  }
  
  private static Proposal newProposal() {
    return new Proposal("X0", new String[0], null, 0);
  }
  
  private static Vote newVote() {
    return new Vote("X0", new Response("c", Intent.ACCEPT, null));
  }
  
  private static Outcome newOutcome() {
    return new Outcome("X0", Resolution.COMMIT, null, new Response[0], null);
  }
}
