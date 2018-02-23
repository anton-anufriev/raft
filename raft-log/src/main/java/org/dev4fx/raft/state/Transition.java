package org.dev4fx.raft.state;

/**
 * Transition from current state, either STEADY (no change) or into a new {@link Role}.
 */
public enum Transition {
    STEADY(null, false),
    TO_FOLLOWER_REPLAY(Role.FOLLOWER, true),
    TO_FOLLOWER_NO_REPLAY(Role.FOLLOWER, false),
    TO_CANDIDATE_NO_REPLAY(Role.CANDIDATE, false),
    TO_LEADER_NO_REPLAY(Role.LEADER, false);

    private final Role targetRole;
    private final boolean replayEvent;

    Transition(final Role targetRole, final boolean replayEvent) {
        this.targetRole = targetRole;//nullable
        this.replayEvent = replayEvent;
    }

    public Role targetRole() {
        return targetRole;
    }

    public final boolean replayEvent() {
        return replayEvent;
    }
}
