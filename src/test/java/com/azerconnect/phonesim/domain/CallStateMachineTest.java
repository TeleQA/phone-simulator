package com.azerconnect.phonesim.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CallStateMachineTest {

    @Test
    void voiceHappyPath() {
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.PENDING, CallStatus.DIALING)).isTrue();
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.DIALING, CallStatus.RINGING)).isTrue();
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.RINGING, CallStatus.ANSWERED)).isTrue();
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.ANSWERED, CallStatus.RELEASED)).isTrue();
    }

    @Test
    void smsSkipsDialingAndRinging() {
        assertThat(CallStateMachine.canTransition(CallKind.SMS, CallStatus.PENDING, CallStatus.ANSWERED)).isTrue();
        assertThat(CallStateMachine.canTransition(CallKind.SMS, CallStatus.PENDING, CallStatus.DIALING)).isFalse();
        assertThat(CallStateMachine.canTransition(CallKind.SMS, CallStatus.ANSWERED, CallStatus.RELEASED)).isTrue();
    }

    @Test
    void anyStateCanFail() {
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.PENDING, CallStatus.FAILED)).isTrue();
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.RINGING, CallStatus.FAILED)).isTrue();
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.ANSWERED, CallStatus.FAILED)).isTrue();
    }

    @Test
    void terminalStatesAreTerminal() {
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.RELEASED, CallStatus.PENDING)).isFalse();
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.FAILED, CallStatus.RELEASED)).isFalse();
    }

    @Test
    void backwardsTransitionRejected() {
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.RINGING, CallStatus.PENDING)).isFalse();
        assertThat(CallStateMachine.canTransition(CallKind.VOICE, CallStatus.ANSWERED, CallStatus.DIALING)).isFalse();
    }

    @Test
    void requireThrowsOnIllegalTransition() {
        assertThatThrownBy(() ->
                CallStateMachine.require(CallKind.VOICE, CallStatus.PENDING, CallStatus.ANSWERED))
                .isInstanceOf(CallStateMachine.IllegalStateTransitionException.class)
                .hasMessageContaining("PENDING -> ANSWERED");
    }
}
