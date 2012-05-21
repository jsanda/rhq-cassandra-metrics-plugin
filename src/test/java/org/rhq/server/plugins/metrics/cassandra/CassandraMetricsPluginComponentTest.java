package org.rhq.server.plugins.metrics.cassandra;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.measurement.DataType;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.MeasurementScheduleRequest;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;

import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
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

    private CassandraMetricsPluginComponent metricsServer;

    @BeforeMethod
    public void initServer() throws Exception {
        metricsServer = new CassandraMetricsPluginComponent();
        metricsServer.initialize(createTestContext());
    }

    @Test
    public void insertNumericData() {
        Cluster cluster = HFactory.getOrCreateCluster("rhq", "localhost:9160");
        Keyspace keyspace = HFactory.createKeyspace("rhq", cluster);

        int scheduleId = 123;

        Mutator<Integer> deleteMutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        deleteMutator.delete(scheduleId, "raw_metrics", null, LongSerializer.get());
        deleteMutator.execute();

        long now = System.currentTimeMillis();
        long oneMinuteAgo = now - MINUTE;
        long twoMinutesAgo = now - (MINUTE * 2);
        long threeMinutesAgo = now - (MINUTE * 3);
        long fiveMinutesAgo = now - (MINUTE * 5);

        String scheduleName = getClass().getName() + "_SCHEDULE";
        long interval = MINUTE * 10;
        boolean enabled = true;
        DataType dataType = DataType.MEASUREMENT;
        MeasurementScheduleRequest request = new MeasurementScheduleRequest(scheduleId, scheduleName, interval,
            enabled, dataType);

        MeasurementReport report = new MeasurementReport();
        report.addData(new MeasurementDataNumeric(threeMinutesAgo, request, 3.2));
        report.addData(new MeasurementDataNumeric(twoMinutesAgo, request, 3.9));
        report.addData(new MeasurementDataNumeric(oneMinuteAgo, request, 2.6));
        report.setCollectionTime(now);

        metricsServer.insertMetrics(report);

        SliceQuery<Integer, Long, Double> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            LongSerializer.get(), DoubleSerializer.get());
        query.setColumnFamily("raw_metrics");
        query.setKey(scheduleId);
        query.setRange(null, null, false, 10);

        QueryResult<ColumnSlice<Long, Double>> queryResult = query.execute();
        List<HColumn<Long, Double>> columns = queryResult.get().getColumns();

        assertEquals(columns.size(), 3, "Expected to get back one column");
    }

    private ServerPluginContext createTestContext() {
        Configuration configuration = new Configuration();
        configuration.put(new PropertySimple("clusterName", "rhq"));
        configuration.put(new PropertySimple("hostIP", "localhost:9160"));
        configuration.put(new PropertySimple("keyspace", "rhq"));
        configuration.put(new PropertySimple("rawMetricsColumnFamily", "raw_metrics"));

        return new ServerPluginContext(null, null, null, configuration, null);
    }

    //@Test
    public void testInsertNumericData() {
        Cluster cluster = HFactory.getOrCreateCluster("rhq", "localhost:9160");
        Keyspace keyspace = HFactory.createKeyspace("rhq", cluster);

        Integer scheduleId = 123;

        Mutator<Integer> deleteMutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        deleteMutator.delete(scheduleId, "raw_metrics", null, LongSerializer.get());
        deleteMutator.execute();

        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        long now = HFactory.createClock();
        mutator.addInsertion(scheduleId, "raw_metrics", HFactory.createColumn(now, 3.23, now, (1000 * 60),
            LongSerializer.get(), DoubleSerializer.get()));
        mutator.execute();

        SliceQuery<Integer, Long, Double> query = HFactory.createSliceQuery(keyspace, IntegerSerializer.get(),
            LongSerializer.get(), DoubleSerializer.get());
        query.setColumnFamily("raw_metrics");
        query.setKey(scheduleId);
        query.setRange(now - 1000, now + 1000, false, 10);

        QueryResult<ColumnSlice<Long, Double>> queryResult = query.execute();
        List<HColumn<Long, Double>> columns = queryResult.get().getColumns();

        assertEquals(columns.size(), 1, "Expected to get back one column");
    }

}
