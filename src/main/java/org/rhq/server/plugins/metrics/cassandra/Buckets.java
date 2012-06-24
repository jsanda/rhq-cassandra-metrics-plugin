package org.rhq.server.plugins.metrics.cassandra;

import org.joda.time.DateTime;

import org.rhq.enterprise.server.measurement.util.MeasurementDataManagerUtility;

/**
 * @author John Sanda
 */
public class Buckets {

    static class Bucket {
        // start time is inclusive
        private long startTime;

        // end time is exclusive
        private long endTime;

        private double sum;
        private double max;
        private double min;
        private int count;

        public Bucket(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public Bucket insert(double value) {
            sum += value;
            if (count == 0) {
                min = value;
                max = value;
            } else if (value < min) {
                min = value;
            } else if (value > max) {
                max = value;
            }
            count++;
            return this;
        }

        public double getAvg() {
            if (count == 0) {
                return Double.NaN;
            }
            return sum / count;
        }

        public double getMax() {
            if (count == 0) {
                return Double.NaN;
            }
            return max;
        }

        public double getMin() {
            if (count == 0) {
                return Double.NaN;
            }
            return min;
        }
    }

    private int numDataPoints = MeasurementDataManagerUtility.DEFAULT_NUM_DATA_POINTS;

    private Bucket[] buckets = new Bucket[numDataPoints];

    private long interval;

    public Buckets(DateTime beginTime, DateTime endTime) {
        this(beginTime.getMillis(), endTime.getMillis());
    }

    public Buckets(long beginTime, long endTime) {
        interval = (endTime - beginTime) / numDataPoints;
        for (int i = 1; i <= numDataPoints; ++i) {
            buckets[i - 1] = new Bucket(beginTime + (interval * (i - 1)), beginTime + (interval * i));
        }
    }

    public int getNumDataPoints() {
        return numDataPoints;
    }

    public long getInterval() {
        return interval;
    }

    public Bucket get(int index) {
        return buckets[index];
    }

    public void insert(long timestamp, double value) {
        for (Bucket bucket : buckets) {
            if (timestamp >= bucket.getStartTime() && timestamp < bucket.getEndTime()) {
                bucket.insert(value);
                return;
            }
        }
    }

}
