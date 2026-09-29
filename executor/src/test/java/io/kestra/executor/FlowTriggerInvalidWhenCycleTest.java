package io.kestra.executor;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.executor.command.Create;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.flows.FlowWithSource;
import io.kestra.core.models.flows.State;
import io.kestra.executor.testkit.Executions;
import io.kestra.executor.testkit.ExecutorTestHarness;
import io.kestra.executor.testkit.Flows;
import io.kestra.executor.testkit.ScriptedWorker;
import io.kestra.executor.testkit.Trace;

import static io.kestra.executor.testkit.HarnessAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives a full executor cycle through the real {@code DefaultExecutor} (with a real
 * {@link io.kestra.executor.FlowTriggerService}) to prove that a Flow trigger whose {@code when} cannot
 * be rendered fires a FAILED execution instead of being silently skipped — the regression behind #10857.
 */
class FlowTriggerInvalidWhenCycleTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final ExecutorTestHarness harness = ExecutorTestHarness.create();

    private static FlowWithSource upstream() {
        return Flows.yaml("""
            id: invalid-when-upstream
            namespace: io.kestra.tests
            tasks:
              - id: hello
                type: io.kestra.plugin.core.log.Log
                message: hi
            """);
    }

    @Test
    void anUnrenderableTriggerWhenFailsTheTriggeredExecution() {
        // Given an upstream flow and a listener whose trigger-level `when` references `namespace`,
        // which is not available at trigger time and therefore cannot be rendered
        FlowWithSource upstream = upstream();
        FlowWithSource listener = Flows.yaml("""
            id: invalid-when-listener
            namespace: io.kestra.tests
            triggers:
              - id: on_upstream
                type: io.kestra.plugin.core.trigger.Flow
                states: [SUCCESS]
                when: "{{ namespace }}"
            tasks:
              - id: noop
                type: io.kestra.plugin.core.log.Log
                message: never reached
            """);
        harness.registerFlow(upstream);
        harness.registerFlow(listener);

        // When the upstream runs to SUCCESS through the whole executor machine
        Execution created = Executions.created(upstream);
        Trace trace = harness.run(created, ScriptedWorker.succeeding(T0));
        assertThat(harness).hasExecutionInState(created, State.Type.SUCCESS);

        // Then the listener trigger produced exactly one execution, and it is FAILED (not silently dropped)
        List<Execution> triggered = trace.emitted("execution")
            .map(emission -> emission.as(Execution.class))
            .filter(execution -> "invalid-when-listener".equals(execution.getFlowId()))
            .toList();
        assertThat(triggered).hasSize(1);
        assertThat(triggered.getFirst().getState().getCurrent()).isEqualTo(State.Type.FAILED);
        assertThat(triggered.getFirst().getTaskRunList()).isNullOrEmpty();
    }

    @Test
    void anUnrenderableDependsOnWhenFailsTheTriggeredExecution() {
        // Given an upstream flow and a listener whose dependsOn entry has an unrenderable `when`
        FlowWithSource upstream = upstream();
        FlowWithSource listener = Flows.yaml("""
            id: invalid-dependson-when-listener
            namespace: io.kestra.tests
            triggers:
              - id: on_upstream
                type: io.kestra.plugin.core.trigger.Flow
                dependsOn:
                  - namespace: io.kestra.tests
                    flowId: invalid-when-upstream
                    states: [SUCCESS]
                    when: "{{ namespace }}"
            tasks:
              - id: noop
                type: io.kestra.plugin.core.log.Log
                message: never reached
            """);
        harness.registerFlow(upstream);
        harness.registerFlow(listener);

        // When the upstream runs to SUCCESS, the dependsOn trigger is evaluated through the multiple-condition cycle
        Execution created = Executions.created(upstream);
        Trace trace = harness.run(created, ScriptedWorker.succeeding(T0));
        assertThat(harness).hasExecutionInState(created, State.Type.SUCCESS);

        // Then the dependsOn trigger produced exactly one execution command, created in a FAILED state
        List<Create> commands = trace.emitted("executionCommand")
            .map(emission -> emission.as(Create.class))
            .filter(command -> "invalid-dependson-when-listener".equals(command.flowId()))
            .toList();
        assertThat(commands).hasSize(1);
        assertThat(commands.getFirst().stateType()).isEqualTo(State.Type.FAILED);
    }
}
