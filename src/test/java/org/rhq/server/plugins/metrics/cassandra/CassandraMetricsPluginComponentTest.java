package org.rhq.server.plugins.metrics.cassandra;

import static java.util.Arrays.asList;
import static org.rhq.test.AssertUtils.assertPropertiesMatch;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
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

    private final long SECOND = 1000;

    private final long MINUTE = 60 * SECOND;

    private final String RAW_METRIC_DATA_CF = "raw_metrics";

    private final String ONE_HOUR_METRIC_DATA_CF = "one_hour_metric_data";

    private final String METRICS_WORK_QUEUE_CF = "metrics_work_queue";

    private CassandraMetricsPluginComponent metricsServer;

    private Keyspace keyspace;

    @BeforeMethod
    public void initServer() throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("rhq", "localhost:9160");
        keyspace = HFactory.createKeyspace("rhq", cluster);

        metricsServer = new CassandraMetricsPluginComponent();
        metricsServer.initialize(createTestContext());
    }

    @Test
    public void insertMultipleRawNumericDataForOneSchedule() {
        int scheduleId = 123;

        purgeDB(scheduleId);

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

        MeasurementReport report = new MeasurementReport();
        report.addData(new MeasurementDataNumeric(threeMinutesAgo.getMillis(), request, 3.2));
        report.addData(new MeasurementDataNumeric(twoMinutesAgo.getMillis(), request, 3.9));
        report.addData(new MeasurementDataNumeric(oneMinuteAgo.getMillis(), request, 2.6));
        report.setCollectionTime(now.getMillis());

        metricsServer.insertMetrics(report);

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
        assertRawMetricsQueueEquals(asList(HFactory.createColumn(expectedComposite, 0, CompositeSerializer.get(),
            IntegerSerializer.get())));
    }

    @Test
    public void calculateOneHourAggregatesForOneSchedule() {
        int scheduleId = 123;

        purgeDB(scheduleId);

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

        MeasurementReport report = new MeasurementReport();
        report.addData(new MeasurementDataNumeric(firstMetricTime.getMillis(), request, 3.2));
        report.addData(new MeasurementDataNumeric(secondMetricTime.getMillis(), request, 3.9));
        report.addData(new MeasurementDataNumeric(thirdMetricTime.getMillis(), request, 2.6));
        report.setCollectionTime(thirdMetricTime.plusMillis(500).getMillis());

        metricsServer.insertMetrics(report);
        metricsServer.calculateAggregates();

        List<HColumn<Composite, Double>> expected = asList(
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MAX), 3.9),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MIN), 2.6),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.AVG), (3.9 + 3.2 + 2.6) / 3));

        assertOneHourDataEquals(scheduleId, expected);
    }

    @Test
    public void calculateOneAggregatesForMultipleSchedules() {
        List<Integer> scheduleIds = asList(123, 456);
        List<String> scheduleNames = asList(getClass().getName() + "_SCHEDULE1", getClass().getName() + "SCHEDULE2");
        long interval = MINUTE * 15;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;

        List<MeasurementScheduleRequest> requests = asList(
            new MeasurementScheduleRequest(scheduleIds.get(0), scheduleNames.get(0), interval, enabled, dataType),
            new MeasurementScheduleRequest(scheduleIds.get(1), scheduleNames.get(1), interval, enabled, dataType));

        purgeDB(scheduleIds);

        DateTime now = new DateTime();
        DateTime lastHour = now.hourOfDay().roundFloorCopy().minusHours(1);
        DateTime firstMetricTime = lastHour.plusMinutes(5);
        DateTime secondMetricTime = lastHour.plusMinutes(10);
        DateTime thirdMetricTime = lastHour.plusMinutes(15);

        MeasurementReport report = new MeasurementReport();
        report.addData(new MeasurementDataNumeric(firstMetricTime.getMillis(), requests.get(0), 1.1));
        report.addData(new MeasurementDataNumeric(secondMetricTime.getMillis(), requests.get(0), 2.2));
        report.addData(new MeasurementDataNumeric(thirdMetricTime.getMillis(), requests.get(0), 3.3));
        report.addData(new MeasurementDataNumeric(firstMetricTime.getMillis(), requests.get(1), 4.4));
        report.addData(new MeasurementDataNumeric(secondMetricTime.getMillis(), requests.get(1), 5.5));
        report.addData(new MeasurementDataNumeric(thirdMetricTime.getMillis(), requests.get(1), 6.6));
        report.setCollectionTime(thirdMetricTime.plusMillis(10).getMillis());

        metricsServer.insertMetrics(report);
        metricsServer.calculateAggregates();

        assertOneHourDataEquals(scheduleIds.get(0), asList(
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MAX), 3.3),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MIN), 1.1),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.AVG), (1.1 + 2.2 + 3.3) / 3)
        ));
        assertOneHourDataEquals(scheduleIds.get(1), asList(
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MAX), 6.6),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.MIN), 4.4),
            HFactory.createColumn(createAggregateKey(lastHour, AggregateType.AVG), (4.4 + 5.5 + 6.6) / 3)
        ));
    }

    private void purgeDB(List<Integer> scheduleIds) {
        purgeDB(scheduleIds.toArray(new Integer[scheduleIds.size()]));
    }

    private void purgeDB(Integer... scheduleIds) {
        purgeQueue();
        purgeNumericMetricsCF(RAW_METRIC_DATA_CF, scheduleIds);
        purgeNumericMetricsCF(ONE_HOUR_METRIC_DATA_CF, scheduleIds);
    }

    private void purgeQueue() {
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());
        queueMutator.delete(RAW_METRIC_DATA_CF, METRICS_WORK_QUEUE_CF, null, CompositeSerializer.get());
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
        configuration.put(new PropertySimple("metricsQueueColumnFamily", METRICS_WORK_QUEUE_CF));

        return new ServerPluginContext(null, null, null, configuration, null);
    }

    private void assertRawMetricsQueueEquals(List<HColumn<Composite, Integer>> expected) {
        SliceQuery<String,Composite, Integer> sliceQuery = HFactory.createSliceQuery(keyspace, StringSerializer.get(),
            new CompositeSerializer().get(), IntegerSerializer.get());
        sliceQuery.setColumnFamily(METRICS_WORK_QUEUE_CF);
        sliceQuery.setKey(RAW_METRIC_DATA_CF);

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

    private void assertOneHourDataEquals(int scheduleId, List<HColumn<Composite, Double>> expected) {
        SliceQuery<Integer, Composite, Double> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            CompositeSerializer.get(), DoubleSerializer.get());
        query.setColumnFamily(ONE_HOUR_METRIC_DATA_CF);
        query.setKey(scheduleId);

        ColumnSliceIterator<Integer, Composite, Double> iterator = new ColumnSliceIterator<Integer, Composite, Double>(
            query, (Composite) null, (Composite) null, false);

        List<HColumn<Composite, Double>> actual = new ArrayList<HColumn<Composite, Double>>();
        while (iterator.hasNext()) {
            actual.add(iterator.next());
        }

        String prefix = "The one hour data for schedule id " + scheduleId + " is wrong.";

        assertEquals(actual.size(), expected.size(), prefix + " The number of columns do not match.");
        int i = 0;
        for (HColumn<Composite, Double> expectedColumn : expected) {
            HColumn<Composite, Double> actualColumn = actual.get(i++);
            assertEquals(getTimestamp(actualColumn.getName()), getTimestamp(expectedColumn.getName()),
                prefix + " The timestamp does not match the expected value.");
            assertEquals(getAggregateType(actualColumn.getName()), getAggregateType(expectedColumn.getName()),
                prefix + " The column data type does not match the expected value");
        }
    }

    private Long getTimestamp(Composite composite) {
        return composite.get(0, LongSerializer.get());
    }

    private Integer getScheduleId(Composite composite) {
        return composite.get(1, IntegerSerializer.get());
    }

    private AggregateType getAggregateType(Composite composite) {
        Integer type = composite.get(1, IntegerSerializer.get());
        return AggregateType.valueOf(type);
    }

    private Composite createAggregateKey(DateTime dateTime, AggregateType type) {
        Composite composite = new Composite();
        composite.addComponent(dateTime.getMillis(), LongSerializer.get());
        composite.addComponent(type.ordinal(), IntegerSerializer.get());

        return composite;
    }

}
