package org.dev4fx.raft.state;

import org.agrona.DirectBuffer;
import org.dev4fx.raft.sbe.*;

public interface ServerState {
    Role role();
    default void onTransition() {}
    default Transition processTick() {return Transition.STEADY;};
    default Transition onVoteRequest(VoteRequestDecoder voteRequestDecoder) {return Transition.STEADY;}
    default Transition onVoteResponse(VoteResponseDecoder voteResponseDecoder) {return Transition.STEADY;}
    default Transition onAppendRequest(AppendRequestDecoder appendRequestDecoder) {return Transition.STEADY;}
    default Transition onAppendResponse(AppendResponseDecoder appendResponseDecoder) {return Transition.STEADY;}
    default Transition onCommandRequest(DirectBuffer buffer, int offset, int length) {return Transition.STEADY;}
    default Transition onTimeoutNow() {return Transition.STEADY;}
}
