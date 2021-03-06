package org.rhq.server.plugins.metrics.cassandra;

import static java.util.Arrays.asList;
import static org.joda.time.DateTime.now;
import static org.rhq.server.plugins.metrics.cassandra.DateTimeService.ONE_MONTH;
import static org.rhq.server.plugins.metrics.cassandra.DateTimeService.ONE_YEAR;
import static org.rhq.server.plugins.metrics.cassandra.DateTimeService.SEVEN_DAYS;
import static org.rhq.server.plugins.metrics.cassandra.DateTimeService.TWO_WEEKS;
import static org.rhq.test.AssertUtils.assertPropertiesMatch;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.chrono.GregorianChronology;
import org.joda.time.field.DividedDateTimeField;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.DisplayType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementDataTrait;
import org.rhq.core.domain.measurement.MeasurementSchedule;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.core.domain.measurement.composite.MeasurementDataNumericHighLowComposite;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * @author John Sanda
 */
public class CassandraMetricsPluginComponentTest {

    private static final boolean ENABLED = true;

    private final long SECOND = 1000;

    private final long MINUTE = 60 * SECOND;

    private final String RAW_METRIC_DATA_CF = "raw_metrics";

    private final String ONE_HOUR_METRIC_DATA_CF = "one_hour_metric_data";

    private final String SIX_HOUR_METRIC_DATA_CF = "six_hour_metric_data";

    private final String TWENTY_FOUR_HOUR_METRIC_DATA_CF = "twenty_four_hour_metric_data";

    private final String METRICS_WORK_QUEUE_CF = "metrics_work_queue";

    private final String TRAITS_CF = "traits";

    private final String RESOURCE_TRAITS_CF = "resource_traits";

    private CassandraMetricsPluginComponentStub metricsServer;

    private Keyspace keyspace;

    private static class CassandraMetricsPluginComponentStub extends CassandraMetricsPluginComponent {

        private DateTime currentHour;

        public void setCurrentHour(DateTime currentHour) {
            this.currentHour = currentHour;
        }

        @Override
        protected DateTime getCurrentHour() {
            if (currentHour == null) {
                return super.getCurrentHour();
            }
            return currentHour;
        }
    }

    @BeforeMethod
    public void initServer() throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("rhq", "127.0.0.1:9160");
        keyspace = HFactory.createKeyspace("rhq", cluster);

        metricsServer = new CassandraMetricsPluginComponentStub();
        metricsServer.initialize(createTestContext());

        purgeDB();
    }

    @Test(enabled = ENABLED)
    public void findRawDataComposites() {
        DateTime beginTime = now().minusHours(4);
        DateTime endTime = now();

        Buckets buckets = new Buckets(beginTime, endTime);

        String scheduleName = getClass().getName() + "_SCHEDULE";
        MeasurementSchedule schedule = new MeasurementSchedule();
        schedule.setId(123);
        long interval = MINUTE * 10;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(schedule.getId(), scheduleName, interval,
            enabled, dataType);

        Set<MeasurementDataNumeric> data = new HashSet<MeasurementDataNumeric>();
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() + 10, request, 1.1));
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() + 20, request, 2.2));
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() + 30, request, 3.3));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + 10, request, 4.4));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + 20, request, 5.5));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + 30, request, 6.6));

        // add some data outside the range
        data.add(new MeasurementDataNumeric(buckets.get(0).getStartTime() - 100, request, 1.23));
        data.add(new MeasurementDataNumeric(buckets.get(59).getStartTime() + buckets.getInterval() + 50, request,
            4.56));

        metricsServer.addNumericData(data);
        List<MeasurementDataNumericHighLowComposite> actualData = metricsServer.findDataForContext(null, null,
            schedule, beginTime.getMillis(), endTime.getMillis());

        assertEquals(actualData.size(), buckets.getNumDataPoints(), "Expected to get back 60 data points.");

        MeasurementDataNumericHighLowComposite expectedBucket0Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(0).getStartTime(), (1.1 + 2.2 + 3.3) / 3, 3.3, 1.1);
        MeasurementDataNumericHighLowComposite expectedBucket59Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(59).getStartTime(), (4.4 + 5.5 + 6.6) / 3, 6.6, 4.4);
        MeasurementDataNumericHighLowComposite expectedBucket29Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(29).getStartTime(), Double.NaN, Double.NaN, Double.NaN);

        assertPropertiesMatch("The data for bucket 0 does not match the expected values.", expectedBucket0Data,
            actualData.get(0));
        assertPropertiesMatch("The data for bucket 59 does not match the expected values.", expectedBucket59Data,
            actualData.get(59));
        assertPropertiesMatch("The data for bucket 29 does not match the expected values.", expectedBucket29Data,
            actualData.get(29));
    }

    @Test(enabled = ENABLED)
    public void find1HourDataComposites() {
        DateTime beginTime = now().minusDays(11);
        DateTime endTime = now();

        Buckets buckets = new Buckets(beginTime, endTime);
        DateTime bucket0Time = new DateTime(buckets.get(0).getStartTime());
        DateTime bucket59Time = new DateTime(buckets.get(59).getStartTime());

        String scheduleName = getClass().getName() + "_SCHEDULE";
        MeasurementSchedule schedule = new MeasurementSchedule();
        schedule.setId(123);
        long interval = MINUTE * 10;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(schedule.getId(), scheduleName, interval,
            enabled, dataType);

        // insert one hour data to be aggregated
        Mutator<Integer> oneHourMutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time, AggregateType.MAX, 3.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time, AggregateType.AVG, 2.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time, AggregateType.MIN, 1.0));

        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time.plusHours(1), AggregateType.MAX, 6.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time.plusHours(1), AggregateType.AVG, 5.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time.plusHours(1), AggregateType.MIN, 4.0));

        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time.plusHours(2), AggregateType.MAX, 3.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time.plusHours(2), AggregateType.AVG, 3.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket0Time.plusHours(2), AggregateType.MIN, 3.0));

        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time, AggregateType.MAX, 9.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time, AggregateType.AVG, 5.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time, AggregateType.MIN, 2.0));

        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time.plusHours(1), AggregateType.MAX, 6.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time.plusHours(1), AggregateType.AVG, 5.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time.plusHours(1), AggregateType.MIN, 4.0));

        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time.plusHours(2), AggregateType.MAX, 3.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time.plusHours(2), AggregateType.AVG, 3.0));
        oneHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create1HourColumn(bucket59Time.plusHours(2), AggregateType.MIN, 3.0));

        oneHourMutator.execute();

        List<MeasurementDataNumericHighLowComposite> actualData = metricsServer.findDataForContext(null, null,
            schedule, beginTime.getMillis(), endTime.getMillis());

        assertEquals(actualData.size(), buckets.getNumDataPoints(), "Expected to get back 60 data points.");

        MeasurementDataNumericHighLowComposite expectedBucket0Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(0).getStartTime(), (2.0 + 5.0 + 3.0) / 3, 5.0, 2.0);
        MeasurementDataNumericHighLowComposite expectedBucket59Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(59).getStartTime(), (5.0 + 5.0 + 3.0) / 3, 5.0, 3.0);
        MeasurementDataNumericHighLowComposite expectedBucket29Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(29).getStartTime(), Double.NaN, Double.NaN, Double.NaN);

        assertPropertiesMatch("The data for bucket 0 does not match the expected values.", expectedBucket0Data,
            actualData.get(0));
        assertPropertiesMatch("The data for bucket 59 does not match the expected values.", expectedBucket59Data,
            actualData.get(59));
        assertPropertiesMatch("The data for bucket 29 does not match the expected values.", expectedBucket29Data,
            actualData.get(29));
    }

    @Test(enabled = ENABLED)
    public void find6HourDataComposites() {
        DateTime beginTime = now().minusDays(20);
        DateTime endTime = now();

        Buckets buckets = new Buckets(beginTime, endTime);
        DateTime bucket0Time = new DateTime(buckets.get(0).getStartTime());
        DateTime bucket59Time = new DateTime(buckets.get(59).getStartTime());

        String scheduleName = getClass().getName() + "_SCHEDULE";
        MeasurementSchedule schedule = new MeasurementSchedule();
        schedule.setId(123);
        long interval = MINUTE * 10;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(schedule.getId(), scheduleName, interval,
            enabled, dataType);

        // insert six hour data to be aggregated
        Mutator<Integer> sixHourMutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time, AggregateType.MAX, 3.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time, AggregateType.AVG, 2.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time, AggregateType.MIN, 1.0));

        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time.plusHours(1), AggregateType.MAX, 6.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time.plusHours(1), AggregateType.AVG, 5.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time.plusHours(1), AggregateType.MIN, 4.0));

        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time.plusHours(2), AggregateType.MAX, 3.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time.plusHours(2), AggregateType.AVG, 3.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket0Time.plusHours(2), AggregateType.MIN, 3.0));

        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time, AggregateType.MAX, 9.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time, AggregateType.AVG, 5.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time, AggregateType.MIN, 2.0));

        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time.plusHours(1), AggregateType.MAX, 6.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time.plusHours(1), AggregateType.AVG, 5.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time.plusHours(1), AggregateType.MIN, 4.0));

        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time.plusHours(2), AggregateType.MAX, 3.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time.plusHours(2), AggregateType.AVG, 3.0));
        sixHourMutator.addInsertion(schedule.getId(), ONE_HOUR_METRIC_DATA_CF,
            create6HourColumn(bucket59Time.plusHours(2), AggregateType.MIN, 3.0));

        sixHourMutator.execute();

        List<MeasurementDataNumericHighLowComposite> actualData = metricsServer.findDataForContext(null, null,
            schedule, beginTime.getMillis(), endTime.getMillis());

        assertEquals(actualData.size(), buckets.getNumDataPoints(), "Expected to get back 60 data points.");

        MeasurementDataNumericHighLowComposite expectedBucket0Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(0).getStartTime(), (2.0 + 5.0 + 3.0) / 3, 5.0, 2.0);
        MeasurementDataNumericHighLowComposite expectedBucket59Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(59).getStartTime(), (5.0 + 5.0 + 3.0) / 3, 5.0, 3.0);
        MeasurementDataNumericHighLowComposite expectedBucket29Data = new MeasurementDataNumericHighLowComposite(
            buckets.get(29).getStartTime(), Double.NaN, Double.NaN, Double.NaN);

        assertPropertiesMatch("The data for bucket 0 does not match the expected values.", expectedBucket0Data,
            actualData.get(0));
        assertPropertiesMatch("The data for bucket 59 does not match the expected values.", expectedBucket59Data,
            actualData.get(59));
        assertPropertiesMatch("The data for bucket 29 does not match the expected values.", expectedBucket29Data,
            actualData.get(29));
    }

    @Test(enabled = ENABLED)
    public void insertMultipleRawNumericDataForOneSchedule() {
        int scheduleId = 123;

        DateTime now = new DateTime();
        DateTime threeMinutesAgo = now.minusMinutes(3);
        DateTime twoMinutesAgo = now.minusMinutes(2);
        DateTime oneMinuteAgo = now.minusMinutes(1);

        int sevenDays = Duration.standardDays(7).toStandardSeconds().getSeconds();

        String scheduleName = getClass().getName() + "_SCHEDULE";
        long interval = MINUTE * 10;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(scheduleId, scheduleName, interval,
            enabled, dataType);

        Set<MeasurementDataNumeric> data = new HashSet<MeasurementDataNumeric>();
        data.add(new MeasurementDataNumeric(threeMinutesAgo.getMillis(), request, 3.2));
        data.add(new MeasurementDataNumeric(twoMinutesAgo.getMillis(), request, 3.9));
        data.add(new MeasurementDataNumeric(oneMinuteAgo.getMillis(), request, 2.6));

        metricsServer.addNumericData(data);

        SliceQuery<Integer, Long, Double> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            LongSerializer.get(), DoubleSerializer.get());
        query.setColumnFamily(RAW_METRIC_DATA_CF);
        query.setKey(scheduleId);
        query.setRange(null, null, false, 10);

        QueryResult<ColumnSlice<Long, Double>> queryResult = query.execute();
        List<HColumn<Long, Double>> actual = queryResult.get().getColumns();

        List<HColumn<Long, Double>> expected = asList(
            HFactory.createColumn(threeMinutesAgo.getMillis(), 3.2, sevenDays, LongSerializer.get(),
                DoubleSerializer.get()),
            HFactory.createColumn(twoMinutesAgo.getMillis(), 3.9, sevenDays, LongSerializer.get(),
                DoubleSerializer.get()),
            HFactory.createColumn(oneMinuteAgo.getMillis(), 2.6, sevenDays, LongSerializer.get(),
                DoubleSerializer.get())
        );

        for (int i = 0; i < expected.size(); ++i) {
            assertPropertiesMatch("The returned columns do not match", expected.get(i), actual.get(i),
                "clock");
        }

        DateTime theHour = now.hourOfDay().roundFloorCopy();
        Composite expectedComposite = new Composite();
        expectedComposite.addComponent(theHour.getMillis(), LongSerializer.get());
        expectedComposite.addComponent(scheduleId, IntegerSerializer.get());

        assert1HourMetricsQueueEquals(asList(HFactory.createColumn(expectedComposite, 0, CompositeSerializer.get(),
            IntegerSerializer.get())));
    }

    @Test(enabled = ENABLED)
    public void insertTraits() {
        DateTime now = new DateTime();
        boolean enabled = true;
        DataType dataType = DataType.TRAIT;
        long interval = MINUTE * 10;

        int scheduleId = 123;
        int definitionId = 456;
        int resourceId = 789;
        String schedule1Name = "TRAIT_1";
        String value1 = "running";
        String value2 = "stopped";
        DisplayType displayType = DisplayType.DETAIL;
        String displayName = "Trait 1";
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(scheduleId, schedule1Name, interval,
            enabled, dataType, null, displayType, displayName, definitionId, resourceId);

        Set<MeasurementDataTrait> data = new HashSet<MeasurementDataTrait>();
        data.add(new MeasurementDataTrait(now.minusMinutes(12).getMillis(), request, value2));
        data.add(new MeasurementDataTrait(now.minusMinutes(2).getMillis(), request, value1));

        metricsServer.addTraitData(data);

        List<HColumn<Long, String>> expected = asList(
            createTraitColumn(now.minusMinutes(12), value2),
            createTraitColumn(now.minusMinutes(2), value1)
        );

        assertTraitDataEquals(scheduleId, expected);

        Composite name1 = new Composite();
        name1.addComponent(now.minusMinutes(12).getMillis(), LongSerializer.get());
        name1.addComponent(scheduleId, IntegerSerializer.get());
        name1.addComponent(definitionId, IntegerSerializer.get());
        name1.addComponent(displayType.ordinal(), IntegerSerializer.get());
        name1.addComponent(displayName, StringSerializer.get());

        Composite name2 = new Composite();
        name2.addComponent(now.minusMinutes(2).getMillis(), LongSerializer.get());
        name2.addComponent(scheduleId, IntegerSerializer.get());
        name2.addComponent(definitionId, IntegerSerializer.get());
        name2.addComponent(displayType.ordinal(), IntegerSerializer.get());
        name2.addComponent(displayName, StringSerializer.get());

        List<HColumn<Composite, String>> expectedIndex = asList(HFactory.createColumn(name1, value2),
            HFactory.createColumn(name2, value1));

        assertResourceTraitIndexEquals(resourceId, expectedIndex);
    }

    //@Test
    public void calculateAggregatesForOneScheduleWhenDBIsEmpty() {
        int scheduleId = 123;

        DateTime now = new DateTime();
        DateTime lastHour = now.hourOfDay().roundFloorCopy().minusHours(1);
        DateTime firstMetricTime = lastHour.plusMinutes(5);
        DateTime secondMetricTime = lastHour.plusMinutes(10);
        DateTime thirdMetricTime = lastHour.plusMinutes(15);

        String scheduleName = getClass().getName() + "_SCHEDULE";
        long interval = MINUTE * 15;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(scheduleId, scheduleName, interval,
            enabled, dataType);

        Set<MeasurementDataNumeric> data = new HashSet<MeasurementDataNumeric>();
        data.add(new MeasurementDataNumeric(firstMetricTime.getMillis(), request, 3.2));
        data.add(new MeasurementDataNumeric(secondMetricTime.getMillis(), request, 3.9));
        data.add(new MeasurementDataNumeric(thirdMetricTime.getMillis(), request, 2.6));

        metricsServer.addNumericData(data);
        metricsServer.calculateAggregates();

        // verify one hour metric data is calculated
        // The ttl for 1 hour data is 14 days.
        int ttl = Days.days(14).toStandardSeconds().getSeconds();
        List<HColumn<Composite, Double>> expected1HourData = asList(
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MAX), 3.9, ttl, CompositeSerializer.get(),
                DoubleSerializer.get()),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MIN), 2.6, ttl, CompositeSerializer.get(),
                DoubleSerializer.get()),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.AVG), (3.9 + 3.2 + 2.6) / 3, ttl,
                CompositeSerializer.get(), DoubleSerializer.get())
         );

        assert1HourDataEquals(scheduleId, expected1HourData);

        // verify six hour metric data is calculated
        List<HColumn<Composite, Double>> expected6HourData = expected1HourData;

        assert6HourDataEquals(scheduleId, expected6HourData);
    }

    @Test(enabled = ENABLED)
    public void aggregateRawDataDuring9thHour() {
        int scheduleId = 123;

        DateTime now = new DateTime();
        DateTime hour0 = now.hourOfDay().roundFloorCopy().minusHours(now.hourOfDay().get());
        DateTime hour9 = hour0.plusHours(9);
        DateTime hour8 = hour9.minusHours(1);

        DateTime firstMetricTime = hour8.plusMinutes(5);
        DateTime secondMetricTime = hour8.plusMinutes(10);
        DateTime thirdMetricTime = hour8.plusMinutes(15);

        double firstValue = 1.1;
        double secondValue = 2.2;
        double thirdValue = 3.3;

        // insert raw data to be aggregated
        Mutator<Integer> rawMetricsMutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        rawMetricsMutator.addInsertion(scheduleId, RAW_METRIC_DATA_CF, createRawDataColumn(firstMetricTime,
            firstValue));
        rawMetricsMutator.addInsertion(scheduleId, RAW_METRIC_DATA_CF,
            createRawDataColumn(secondMetricTime, secondValue));
        rawMetricsMutator.addInsertion(scheduleId, RAW_METRIC_DATA_CF, createRawDataColumn(thirdMetricTime,
            thirdValue));

        rawMetricsMutator.execute();

        // update the one hour queue
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());
        Composite key = createQueueColumnName(hour8, scheduleId);
        HColumn<Composite, Integer> oneHourQueueColumn = HFactory.createColumn(key, 0, CompositeSerializer.get(),
            IntegerSerializer.get());
        queueMutator.addInsertion(ONE_HOUR_METRIC_DATA_CF, METRICS_WORK_QUEUE_CF, oneHourQueueColumn);

        queueMutator.execute();

        metricsServer.setCurrentHour(hour9);
        metricsServer.calculateAggregates();

        // verify that the 1 hour aggregates are calculated

        assert1HourDataEquals(scheduleId, asList(
            create1HourColumn(hour8, AggregateType.MAX, thirdValue),
            create1HourColumn(hour8, AggregateType.MIN, firstValue),
            create1HourColumn(hour8, AggregateType.AVG, (firstValue + secondValue + thirdValue) / 3)
        ));

        Chronology chronology = GregorianChronology.getInstance();
        DateTimeField hourField = chronology.hourOfDay();
        DividedDateTimeField dividedField = new DividedDateTimeField(hourField, DateTimeFieldType.clockhourOfDay(), 6);
        long timestamp = dividedField.roundFloor(hour9.getMillis());
        DateTime sixHourSlice = new DateTime(timestamp);

        // verify that the 6 hour queue is updated
        assert6HourMetricsQueueEquals(asList(HFactory.createColumn(createQueueColumnName(sixHourSlice, scheduleId), 0,
            CompositeSerializer.get(), IntegerSerializer.get())));

        // The 6 hour data should not get aggregated since the current 6 hour time slice
        // has not passed yet. More specifically, the aggregation job is running at 09:00
        // which means that the current 6 hour slice is from 06:00 to 12:00.
        assert6HourDataEmpty(scheduleId);

        // verify that the 24 hour queue is empty
        assert24HourMetricsQueueEmpty(scheduleId);

        // verify that the 1 hour queue has been purged
        assert1HourMetricsQueueEmpty(scheduleId);
    }

    @Test(enabled = ENABLED)
    public void aggregate1HourDataDuring12thHour() {
        // set up the test fixture
        int scheduleId = 123;

        DateTime now = new DateTime();
        DateTime hour0 = now.hourOfDay().roundFloorCopy().minusHours(now.hourOfDay().get());
        DateTime hour12 = hour0.plusHours(12);
        DateTime hour6 = hour0.plusHours(6);
        DateTime hour7 = hour0.plusHours(7);
        DateTime hour8 = hour0.plusHours(8);

        double min1 = 1.1;
        double avg1 = 2.2;
        //double max1 = 3.3;
        double max1 = 9.9;

        double min2 = 4.4;
        double avg2 = 5.5;
        double max2 = 6.6;

        // insert one hour data to be aggregated
        Mutator<Integer> oneHourMutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        oneHourMutator.addInsertion(scheduleId, ONE_HOUR_METRIC_DATA_CF, create1HourColumn(hour7, AggregateType.MAX,
            max1));
        oneHourMutator.addInsertion(scheduleId, ONE_HOUR_METRIC_DATA_CF, create1HourColumn(hour7, AggregateType.MIN,
            min1));
        oneHourMutator.addInsertion(scheduleId, ONE_HOUR_METRIC_DATA_CF, create1HourColumn(hour7, AggregateType.AVG,
            avg1));
        oneHourMutator.addInsertion(scheduleId, ONE_HOUR_METRIC_DATA_CF, create1HourColumn(hour8, AggregateType.MAX,
            max2));
        oneHourMutator.addInsertion(scheduleId, ONE_HOUR_METRIC_DATA_CF, create1HourColumn(hour8, AggregateType.MIN,
            min2));
        oneHourMutator.addInsertion(scheduleId, ONE_HOUR_METRIC_DATA_CF, create1HourColumn(hour8, AggregateType.AVG,
            avg2));
        oneHourMutator.execute();

        // update the 6 hour queue
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());
        Composite key = createQueueColumnName(hour6, scheduleId);
        HColumn<Composite, Integer> sixHourQueueColumn = HFactory.createColumn(key, 0, CompositeSerializer.get(),
            IntegerSerializer.get());
        queueMutator.addInsertion(SIX_HOUR_METRIC_DATA_CF, METRICS_WORK_QUEUE_CF, sixHourQueueColumn);

        queueMutator.execute();

        // execute the system under test
        metricsServer.setCurrentHour(hour12);
        metricsServer.calculateAggregates();

        // verify the results
        // verify that the one hour data has been aggregated
        assert6HourDataEquals(scheduleId, asList(
            create6HourColumn(hour6, AggregateType.MAX, max1),
            create6HourColumn(hour6, AggregateType.MIN, min1),
            create6HourColumn(hour6, AggregateType.AVG, (avg1 + avg2) / 2)
        ));

        // verify that the 6 hour queue has been updated
        assert6HourMetricsQueueEmpty(scheduleId);

        // verify that the 24 hour queue is updated
        assert24HourMetricsQueueEquals(asList(HFactory.createColumn(createQueueColumnName(hour0, scheduleId), 0,
            CompositeSerializer.get(), IntegerSerializer.get())));

        // verify that 6 hour data is not rolled up into the 24 hour bucket
        assert24HourDataEmpty(scheduleId);
    }

    @Test(enabled = ENABLED)
    public void aggregate6HourDataDuring24thHour() {
        // set up the test fixture
        int scheduleId = 123;

        DateTime now = new DateTime();
        DateTime hour0 = now.hourOfDay().roundFloorCopy().minusHours(now.hourOfDay().get());
        DateTime hour12 = hour0.plusHours(12);
        DateTime hour6 = hour0.plusHours(6);
        DateTime hour24 = hour0.plusHours(24);

        double min1 = 1.1;
        double avg1 = 2.2;
        double max1 = 3.3;

        double min2 = 4.4;
        double avg2 = 5.5;
        double max2 = 6.6;

        // insert 6 hour data to be aggregated
        Mutator<Integer> sixHourMutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        sixHourMutator.addInsertion(scheduleId, SIX_HOUR_METRIC_DATA_CF, create6HourColumn(hour6, AggregateType.MAX,
            max1));
        sixHourMutator.addInsertion(scheduleId, SIX_HOUR_METRIC_DATA_CF, create6HourColumn(hour6, AggregateType.MIN,
            min1));
        sixHourMutator.addInsertion(scheduleId, SIX_HOUR_METRIC_DATA_CF, create6HourColumn(hour6, AggregateType.AVG,
            avg1));
        sixHourMutator.addInsertion(scheduleId, SIX_HOUR_METRIC_DATA_CF, create6HourColumn(hour12, AggregateType.MAX,
            max2));
        sixHourMutator.addInsertion(scheduleId, SIX_HOUR_METRIC_DATA_CF, create6HourColumn(hour12, AggregateType.MIN,
            min2));
        sixHourMutator.addInsertion(scheduleId, SIX_HOUR_METRIC_DATA_CF, create6HourColumn(hour12, AggregateType.AVG,
            avg2));
        sixHourMutator.execute();

        // update the 24 queue
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());
        Composite key = createQueueColumnName(hour0, scheduleId);
        HColumn<Composite, Integer> twentyFourHourQueueColumn = HFactory.createColumn(key, 0, CompositeSerializer.get(),
            IntegerSerializer.get());
        queueMutator.addInsertion(TWENTY_FOUR_HOUR_METRIC_DATA_CF, METRICS_WORK_QUEUE_CF, twentyFourHourQueueColumn);

        queueMutator.execute();

        // execute the system under test
        metricsServer.setCurrentHour(hour24);
        metricsServer.calculateAggregates();

        // verify the results
        // verify that the 6 hour data is aggregated
        assert24HourDataEquals(scheduleId, asList(
            create24HourColumn(hour0, AggregateType.MAX, max2),
            create24HourColumn(hour0, AggregateType.MIN, min1),
            create24HourColumn(hour0, AggregateType.AVG, (avg1 + avg2) / 2)
        ));

        // verify that the 24 hour queue is updated
        assert24HourMetricsQueueEmpty(scheduleId);
    }

    private HColumn<Long, Double> createRawDataColumn(DateTime timestamp, double value) {
        return HFactory.createColumn(timestamp.getMillis(), value, SEVEN_DAYS, LongSerializer.get(),
            DoubleSerializer.get());
    }

    private void purgeDB() {
        DAO dao = new DAO(keyspace);
        dao.deleteAllRows(METRICS_WORK_QUEUE_CF, StringSerializer.get());
        dao.deleteAllRows(RAW_METRIC_DATA_CF, IntegerSerializer.get());
        dao.deleteAllRows(ONE_HOUR_METRIC_DATA_CF, IntegerSerializer.get());
        dao.deleteAllRows(SIX_HOUR_METRIC_DATA_CF, IntegerSerializer.get());
        dao.deleteAllRows(TWENTY_FOUR_HOUR_METRIC_DATA_CF, IntegerSerializer.get());
        dao.deleteAllRows(TRAITS_CF, IntegerSerializer.get());
        dao.deleteAllRows(RESOURCE_TRAITS_CF, IntegerSerializer.get());
    }

    private void purgeQueue() {
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());
        queueMutator.delete(ONE_HOUR_METRIC_DATA_CF, METRICS_WORK_QUEUE_CF, null, CompositeSerializer.get());
        queueMutator.delete(SIX_HOUR_METRIC_DATA_CF, METRICS_WORK_QUEUE_CF, null, CompositeSerializer.get());
        queueMutator.delete(TWENTY_FOUR_HOUR_METRIC_DATA_CF, METRICS_WORK_QUEUE_CF, null, CompositeSerializer.get());
        queueMutator.execute();
    }

    private void purgeNumericMetricsCF(String columnFamily, Integer... scheduleIds) {
        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        for (int id : scheduleIds) {
            mutator.addDeletion(id, columnFamily, null, LongSerializer.get());
        }
        mutator.execute();
    }

    private ServerPluginContext createTestContext() {
        Configuration configuration = new Configuration();
        configuration.put(new PropertySimple("clusterName", "rhq"));
        configuration.put(new PropertySimple("hostIP", "localhost:9160"));
        configuration.put(new PropertySimple("keyspace", "rhq"));
        configuration.put(new PropertySimple("rawMetricsColumnFamily", RAW_METRIC_DATA_CF));
        configuration.put(new PropertySimple("oneHourMetricsColumnFamily", ONE_HOUR_METRIC_DATA_CF));
        configuration.put(new PropertySimple("sixHourMetricsColumnFamily", SIX_HOUR_METRIC_DATA_CF));
        configuration.put(new PropertySimple("twentyFourHourMetricsColumnFamily", TWENTY_FOUR_HOUR_METRIC_DATA_CF));
        configuration.put(new PropertySimple("metricsQueueColumnFamily", METRICS_WORK_QUEUE_CF));
        configuration.put(new PropertySimple("traitsColumnFamily", TRAITS_CF));
        configuration.put(new PropertySimple("resourceTraitsColumnFamily", RESOURCE_TRAITS_CF));

        return new ServerPluginContext(null, null, null, configuration, null);
    }

    private void assert1HourMetricsQueueEquals(List<HColumn<Composite, Integer>> expected) {
        assertMetricsQueueEquals(ONE_HOUR_METRIC_DATA_CF, expected);
    }

    private void assert6HourMetricsQueueEquals(List<HColumn<Composite, Integer>> expected) {
        assertMetricsQueueEquals(SIX_HOUR_METRIC_DATA_CF, expected);
    }

    private void assert24HourMetricsQueueEquals(List<HColumn<Composite, Integer>> expected) {
        assertMetricsQueueEquals(TWENTY_FOUR_HOUR_METRIC_DATA_CF, expected);
    }

    private void assertMetricsQueueEquals(String columnFamily, List<HColumn<Composite, Integer>> expected) {
        SliceQuery<String,Composite, Integer> sliceQuery = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
            new CompositeSerializer().get(), IntegerSerializer.get());
        sliceQuery.setColumnFamily(METRICS_WORK_QUEUE_CF);
        sliceQuery.setKey(columnFamily);

        ColumnSliceIterator<String, Composite, Integer> iterator = new ColumnSliceIterator<String, Composite, Integer>(
            sliceQuery, (Composite) null, (Composite) null, false);

        List<HColumn<Composite, Integer>> actual = new ArrayList<HColumn<Composite, Integer>>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        assertEquals(actual.size(), expected.size(), "The number of entries in the queue do not match.");
        int i = 0;
        for (HColumn<Composite, Integer> expectedColumn :  expected) {
            HColumn<Composite, Integer> actualColumn = actual.get(i++);
            assertEquals(getTimestamp(actualColumn.getName()), getTimestamp(expectedColumn.getName()),
                "The timestamp does not match the expected value.");
            assertEquals(getScheduleId(actualColumn.getName()), getScheduleId(expectedColumn.getName()),
                "The schedule id does not match the expected value.");
        }
    }

    private void assert1HourMetricsQueueEmpty(int scheduleId) {
        assertMetricsQueueEmpty(scheduleId, ONE_HOUR_METRIC_DATA_CF);
    }

    private void assert6HourMetricsQueueEmpty(int scheduleId) {
        assertMetricsQueueEmpty(scheduleId, SIX_HOUR_METRIC_DATA_CF);
    }

    private void assert24HourMetricsQueueEmpty(int scheduleId) {
        assertMetricsQueueEmpty(scheduleId, TWENTY_FOUR_HOUR_METRIC_DATA_CF);
    }

    private void assertMetricsQueueEmpty(int scheduleId, String columnFamily) {
        SliceQuery<String,Composite, Integer> sliceQuery = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
            new CompositeSerializer().get(), IntegerSerializer.get());
        sliceQuery.setColumnFamily(METRICS_WORK_QUEUE_CF);
        sliceQuery.setKey(columnFamily);

        ColumnSliceIterator<String, Composite, Integer> iterator = new ColumnSliceIterator<String, Composite, Integer>(
            sliceQuery, (Composite) null, (Composite) null, false);

        List<HColumn<Composite, Integer>> actual = new ArrayList<HColumn<Composite, Integer>>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        String queueName;
        if (columnFamily.equals(ONE_HOUR_METRIC_DATA_CF)) {
            queueName = "1 hour";
        } else if (columnFamily.equals(SIX_HOUR_METRIC_DATA_CF)) {
            queueName = "6 hour";
        } else if (columnFamily.equals(TWENTY_FOUR_HOUR_METRIC_DATA_CF)) {
            queueName = "24 hour";
        } else {
            throw new IllegalArgumentException(columnFamily + " is not a recognized metric data column family.");
        }

        assertEquals(actual.size(), 0, "Expected the " + queueName + " queue to be empty for schedule id " +
            scheduleId);
    }

    private void assert1HourDataEquals(int scheduleId, List<HColumn<Composite, Double>> expected) {
        assertMetricDataEquals(scheduleId, ONE_HOUR_METRIC_DATA_CF, expected);
    }

    private void assert6HourDataEquals(int scheduleId, List<HColumn<Composite, Double>> expected) {
        assertMetricDataEquals(scheduleId, SIX_HOUR_METRIC_DATA_CF, expected);
    }

    private void assert24HourDataEquals(int scheduleId, List<HColumn<Composite, Double>> expected) {
        assertMetricDataEquals(scheduleId, TWENTY_FOUR_HOUR_METRIC_DATA_CF, expected);
    }

    private void assertMetricDataEquals(int scheduleId, String columnFamily, List<HColumn<Composite,
        Double>> expected) {
        SliceQuery<Integer, Composite, Double> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            CompositeSerializer.get(), DoubleSerializer.get());
        query.setColumnFamily(columnFamily);
        query.setKey(scheduleId);

        ColumnSliceIterator<Integer, Composite, Double> iterator = new ColumnSliceIterator<Integer, Composite, Double>(
            query, (Composite) null, (Composite) null, false);

        List<HColumn<Composite, Double>> actual = new ArrayList<HColumn<Composite, Double>>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        String prefix;
        if (columnFamily.equals(ONE_HOUR_METRIC_DATA_CF)) {
            prefix = "The one hour data for schedule id " + scheduleId + " is wrong.";
        } else if (columnFamily.equals(SIX_HOUR_METRIC_DATA_CF)) {
            prefix = "The six hour data for schedule id " + scheduleId + " is wrong.";
        } else if (columnFamily.equals(TWENTY_FOUR_HOUR_METRIC_DATA_CF)) {
            prefix = "The twenty-four hour data for schedule id " + scheduleId + " is wrong.";
        } else {
            throw new IllegalArgumentException(columnFamily + " is not a recognized column family");
        }

        assertEquals(actual.size(), expected.size(), prefix + " The number of columns do not match.");
        int i = 0;
        for (HColumn<Composite, Double> expectedColumn : expected) {
            HColumn<Composite, Double> actualColumn = actual.get(i++);
            assertEquals(getTimestamp(actualColumn.getName()), getTimestamp(expectedColumn.getName()),
                prefix + " The timestamp does not match the expected value.");
            assertEquals(getAggregateType(actualColumn.getName()), getAggregateType(expectedColumn.getName()),
                prefix + " The column data type does not match the expected value");
            assertEquals(actualColumn.getValue(), expectedColumn.getValue(), "The column value is wrong");
            assertEquals(actualColumn.getTtl(), expectedColumn.getTtl(), "The ttl for the column is wrong.");
        }
    }

    private void assertTraitDataEquals(int scheduleId, List<HColumn<Long, String>> expected) {
        SliceQuery<Integer, Long, String> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            LongSerializer.get(), StringSerializer.get());
        query.setColumnFamily(TRAITS_CF);
        query.setKey(scheduleId);

        ColumnSliceIterator<Integer, Long, String> iterator = new ColumnSliceIterator<Integer, Long, String>(query,
            (Long) null, (Long) null, false);

        List<HColumn<Long, String>> actual = new ArrayList<HColumn<Long, String>>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        assertEquals(actual.size(), expected.size(), "The number of columns in the " + TRAITS_CF + " CF do not match");
        for (int i = 0; i < expected.size(); ++i) {
            assertPropertiesMatch("The returned columns do not match", expected.get(i), actual.get(i), "clock");
        }
    }

    private void assertResourceTraitIndexEquals(int resourceId, List<HColumn<Composite, String>> expected) {
        SliceQuery<Integer, Composite, String> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            CompositeSerializer.get(), StringSerializer.get());
        query.setColumnFamily(RESOURCE_TRAITS_CF);
        query.setKey(resourceId);

        ColumnSliceIterator<Integer, Composite, String> iterator = new ColumnSliceIterator<Integer, Composite, String>(
            query, (Composite) null, (Composite) null, false);

        List<HColumn<Composite, String>> actual = new ArrayList<HColumn<Composite, String>>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        assertEquals(actual.size(), expected.size(), "The number of columns in " + RESOURCE_TRAITS_CF +
            " do not match for resource id " + resourceId);
        int i = 0;
        for (HColumn<Composite, String> expectedColumn : expected) {
            HColumn<Composite, String> actualColumn = actual.get(i++);
            Composite expectedName = expectedColumn.getName();
            Composite actualName = actualColumn.getName();

            // check timestamp
            assertEquals(get(actualName, 0, Long.class), get(expectedName, 0, Long.class), "The timestamp is wrong");

            // check schedule id
            assertEquals(get(actualName, 1, Integer.class), get(expectedName, 1, Integer.class),
                "The schedule id is wrong");

            // check definition id
            assertEquals(get(actualName, 2, Integer.class), get(expectedName, 2, Integer.class),
                "The definition id is wrong");

            // check the display type
            assertEquals(get(actualName, 3, Integer.class), get(expectedName, 3, Integer.class),
                "The display type is wrong");

            // check the display name
            assertEquals(get(actualName, 4, String.class), get(expectedName, 4, String.class),
                "The display name is wrong");

            // check the value
            assertEquals(actualColumn.getValue(), expectedColumn.getValue(), "The value is wrong");
        }
    }

    private void assert6HourDataEmpty(int scheduleId) {
        assertMetricDataEmpty(scheduleId, SIX_HOUR_METRIC_DATA_CF);
    }

    private void assert24HourDataEmpty(int scheduleId) {
        assertMetricDataEmpty(scheduleId, TWENTY_FOUR_HOUR_METRIC_DATA_CF);
    }

    private void assertMetricDataEmpty(int scheduleId, String columnFamily) {
        SliceQuery<Integer, Composite, Double> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            CompositeSerializer.get(), DoubleSerializer.get());
        query.setColumnFamily(columnFamily);
        query.setKey(scheduleId);

        ColumnSliceIterator<Integer, Composite, Double> iterator = new ColumnSliceIterator<Integer, Composite, Double>(
            query, (Composite) null, (Composite) null, false);

        List<HColumn<Composite, Double>> actual = new ArrayList<HColumn<Composite, Double>>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        String prefix;
        if (columnFamily.equals(ONE_HOUR_METRIC_DATA_CF)) {
            prefix = "The one hour data for schedule id " + scheduleId + " is wrong.";
        } else if (columnFamily.equals(SIX_HOUR_METRIC_DATA_CF)) {
            prefix = "The six hour data for schedule id " + scheduleId + " is wrong.";
        } else if (columnFamily.equals(TWENTY_FOUR_HOUR_METRIC_DATA_CF)) {
            prefix = "The twenty-four hour data for schedule id " + scheduleId + " is wrong.";
        } else {
            throw new IllegalArgumentException(columnFamily + " is not a recognized column family");
        }

        assertEquals(actual.size(), 0, prefix + " Expected the row to be empty.");
    }

    private Long getTimestamp(Composite composite) {
        return composite.get(0, LongSerializer.get());
    }

    private Integer getScheduleId(Composite composite) {
        return composite.get(1, IntegerSerializer.get());
    }

    private <T> T get(Composite composite, int index, Class<T> serializerType) {
        Serializer<T> serializer = SerializerTypeInferer.getSerializer(serializerType);
        return composite.get(index, serializer);
    }

    private AggregateType getAggregateType(Composite composite) {
        Integer type = composite.get(1, IntegerSerializer.get());
        return AggregateType.valueOf(type);
    }

    private HColumn<Long, String> createTraitColumn(DateTime dateTime, String value) {
        return HFactory.createColumn(dateTime.getMillis(), value, ONE_YEAR, LongSerializer.get(),
            StringSerializer.get());
    }

    private HColumn<Composite, Double> create1HourColumn(DateTime dateTime, AggregateType type, double value) {
        return HFactory.createColumn(createAggregateKey(dateTime, type), value, TWO_WEEKS, CompositeSerializer.get(),
            DoubleSerializer.get());
    }

    private HColumn<Composite, Double> create6HourColumn(DateTime dateTime, AggregateType type, double value) {
        return HFactory.createColumn(createAggregateKey(dateTime, type), value, ONE_MONTH, CompositeSerializer.get(),
            DoubleSerializer.get());
    }

    private HColumn<Composite, Double> create24HourColumn(DateTime dateTime, AggregateType type, double value) {
        return HFactory.createColumn(createAggregateKey(dateTime, type), value, ONE_YEAR, CompositeSerializer.get(),
            DoubleSerializer.get());
    }

    private Composite createAggregateKey(DateTime dateTime, AggregateType type) {
        Composite composite = new Composite();
        composite.addComponent(dateTime.getMillis(), LongSerializer.get());
        composite.addComponent(type.ordinal(), IntegerSerializer.get());

        return composite;
    }

    private Composite createQueueColumnName(DateTime dateTime, int scheduleId) {
        Composite composite = new Composite();
        composite.addComponent(dateTime.getMillis(), LongSerializer.get());
        composite.addComponent(scheduleId, IntegerSerializer.get());

        return composite;
    }

}
