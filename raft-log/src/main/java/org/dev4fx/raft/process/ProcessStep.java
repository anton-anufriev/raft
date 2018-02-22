package org.dev4fx.raft.process;

public interface ProcessStep {
    boolean execute();

    default boolean finalise() {
        return !execute();
    }

    static ProcessStep finalisable(final ProcessStep step) {
        return step::execute;
    }

    static ProcessStep nonFinalisable(final ProcessStep step) {
        return new ProcessStep() {
            @Override
            public boolean execute() {
                return step.execute();
            }

            @Override
            public boolean finalise() {
                step.execute();
                return true;
            }
        };
    }
}
