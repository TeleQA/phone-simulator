package com.azerconnect.phonesim.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public final class CallStateMachine {

    private static final Map<CallStatus, Set<CallStatus>> VOICE = new EnumMap<>(CallStatus.class);
    private static final Map<CallStatus, Set<CallStatus>> SMS = new EnumMap<>(CallStatus.class);

    static {
        VOICE.put(CallStatus.PENDING, EnumSet.of(CallStatus.DIALING, CallStatus.FAILED));
        VOICE.put(CallStatus.DIALING, EnumSet.of(CallStatus.RINGING, CallStatus.FAILED));
        VOICE.put(CallStatus.RINGING, EnumSet.of(CallStatus.ANSWERED, CallStatus.FAILED));
        VOICE.put(CallStatus.ANSWERED, EnumSet.of(CallStatus.RELEASED, CallStatus.FAILED));
        VOICE.put(CallStatus.RELEASED, EnumSet.noneOf(CallStatus.class));
        VOICE.put(CallStatus.FAILED, EnumSet.noneOf(CallStatus.class));

        SMS.put(CallStatus.PENDING, EnumSet.of(CallStatus.ANSWERED, CallStatus.FAILED));
        SMS.put(CallStatus.ANSWERED, EnumSet.of(CallStatus.RELEASED, CallStatus.FAILED));
        SMS.put(CallStatus.RELEASED, EnumSet.noneOf(CallStatus.class));
        SMS.put(CallStatus.FAILED, EnumSet.noneOf(CallStatus.class));
        SMS.put(CallStatus.DIALING, EnumSet.noneOf(CallStatus.class));
        SMS.put(CallStatus.RINGING, EnumSet.noneOf(CallStatus.class));
    }

    private CallStateMachine() {}

    public static boolean canTransition(CallKind kind, CallStatus from, CallStatus to) {
        Map<CallStatus, Set<CallStatus>> table = (kind == CallKind.SMS) ? SMS : VOICE;
        return table.getOrDefault(from, EnumSet.noneOf(CallStatus.class)).contains(to);
    }

    public static CallStatus require(CallKind kind, CallStatus from, CallStatus to) {
        if (!canTransition(kind, from, to)) {
            throw new IllegalStateTransitionException(kind, from, to);
        }
        return to;
    }

    public static class IllegalStateTransitionException extends RuntimeException {
        public IllegalStateTransitionException(CallKind kind, CallStatus from, CallStatus to) {
            super("Illegal " + kind + " transition: " + from + " -> " + to);
        }
    }
}
