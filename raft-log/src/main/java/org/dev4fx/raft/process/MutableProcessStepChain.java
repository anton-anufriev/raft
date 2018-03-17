package org.dev4fx.raft.process;

import java.util.function.BiFunction;

public class MutableProcessStepChain {
    private static final BiFunction<ProcessStep, ProcessStep, ProcessStep> THEN = ProcessStep::then;
    private static final BiFunction<ProcessStep, ProcessStep, ProcessStep> THEN_IF_WORK_DONE = ProcessStep::thenIfWorkDone;
    private static final BiFunction<ProcessStep, ProcessStep, ProcessStep> THEN_IF_WORK_NOT_DONE = ProcessStep::thenIfWorkNotDone;

    private ProcessStep runningStep;

    public void addStep(final ProcessStep newStep, final BiFunction<ProcessStep, ProcessStep, ProcessStep> transformation) {
        runningStep = (runningStep == null) ? newStep : transformation.apply(runningStep, newStep);
    }

    public void thenStep(final ProcessStep newStep) {
        addStep(newStep, THEN);
    }

    public void thenStepIfWorkDone(final ProcessStep newStep) {
        addStep(newStep, THEN_IF_WORK_DONE);
    }

    public void thenStepIfWorkNotDone(final ProcessStep newStep) {
        addStep(newStep, THEN_IF_WORK_NOT_DONE);
    }

    public ProcessStep getOrNull() {
        return runningStep;
    }

    public static MutableProcessStepChain create() {
        return new MutableProcessStepChain();
    }
}