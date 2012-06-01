package org.rhq.server.plugins.metrics.cassandra;

/**
 * @author John Sanda
 */
public enum AggregateType {
    MAX, MIN, AVG;

    public static AggregateType valueOf(int type) {
        switch (type) {
            case 0 : return MAX;
            case 1 : return MIN;
            case 2 : return AVG;
            default: throw new IllegalArgumentException(type + " is not a supported " +
                AggregateType.class.getSimpleName());
        }
    }
}
