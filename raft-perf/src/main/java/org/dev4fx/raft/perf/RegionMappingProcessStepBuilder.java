package org.dev4fx.raft.perf;

import org.dev4fx.raft.mmap.api.Processor;
import org.dev4fx.raft.process.MutableProcessStepChain;
import org.dev4fx.raft.process.ProcessStep;

import java.util.function.Consumer;

public class RegionMappingProcessStepBuilder implements Consumer<Processor>{
    private final MutableProcessStepChain processStepChain = new MutableProcessStepChain();

    @Override
    public void accept(final Processor processor) {
        processStepChain.thenStep(processor::process);
    }

    public ProcessStep build() {
        final ProcessStep processStep = processStepChain.getOrNull();
        return processStep == null ? ProcessStep.NO_OP : processStep;
    }
}
