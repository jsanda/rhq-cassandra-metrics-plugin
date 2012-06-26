package org.rhq.server.plugins.metrics.cassandra;

import static org.joda.time.DateTime.now;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Duration;
import org.joda.time.Minutes;
import org.joda.time.chrono.GregorianChronology;
import org.joda.time.field.DividedDateTimeField;

/**
 * @author John Sanda
 */
public class DateTimeService {

    static final int SEVEN_DAYS = Duration.standardDays(7).toStandardSeconds().getSeconds();
    static final int TWO_WEEKS = Duration.standardDays(14).toStandardSeconds().getSeconds();
    static final int ONE_MONTH = Duration.standardDays(31).toStandardSeconds().getSeconds();
    static final int ONE_YEAR = Duration.standardDays(365).toStandardSeconds().getSeconds();

    private DateTimeComparator dateTimeComparator = DateTimeComparator.getInstance();

    public DateTime getTimeSlice(DateTime dateTime, Minutes interval) {
        Chronology chronology = GregorianChronology.getInstance();
        DateTimeField hourField = chronology.hourOfDay();
        DividedDateTimeField dividedField = new DividedDateTimeField(hourField, DateTimeFieldType.clockhourOfDay(),
            interval.toStandardHours().getHours());
        long timestamp = dividedField.roundFloor(dateTime.getMillis());

        return new DateTime(timestamp);
    }

    public boolean isInRawDataRange(DateTime dateTime) {
        return dateTimeComparator.compare(now().minusDays(7), dateTime) < 0;
    }

    public boolean isIn1HourDataRange(DateTime dateTime) {
        return dateTimeComparator.compare(now().minusDays(14), dateTime) < 0;
    }

    public boolean isIn6HourDataRnage(DateTime dateTime) {
        return dateTimeComparator.compare(now().minusDays(31), dateTime) < 0;
    }
}
