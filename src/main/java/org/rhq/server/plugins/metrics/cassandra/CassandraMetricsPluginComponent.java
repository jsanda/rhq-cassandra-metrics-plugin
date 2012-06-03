package org.rhq.server.plugins.metrics.cassandra;

import java.util.Set;
import java.util.TreeSet;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.enterprise.server.plugin.pc.ServerPluginComponent;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginFacet;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DoubleSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * @author John Sanda
 */
public class CassandraMetricsPluginComponent implements MetricsServerPluginFacet, ServerPluginComponent {

    private Cluster cluster;

    private String keyspaceName;

    private String rawMetricsDataCF;

    private String oneHourMetricsDataCF;

    private String sixHourMetricsDataCF;

    private String twentyFourHourMetricsDataCF;

    private String metricsQueueCF;

    @Override
    public void initialize(ServerPluginContext serverPluginContext) throws Exception {
        Configuration pluginConfig = serverPluginContext.getPluginConfiguration();

        cluster = HFactory.getOrCreateCluster(pluginConfig.getSimpleValue("clusterName"),
            pluginConfig.getSimpleValue("hostIP"));
        keyspaceName = pluginConfig.getSimpleValue("keyspace");
        rawMetricsDataCF = pluginConfig.getSimpleValue("rawMetricsColumnFamily");
        oneHourMetricsDataCF = pluginConfig.getSimpleValue("oneHourMetricsColumnFamily");
        sixHourMetricsDataCF = pluginConfig.getSimpleValue("sixHourMetricsColumnFamily");
        twentyFourHourMetricsDataCF = pluginConfig.getSimpleValue("twentyFourHourMetricsColumnFamily");
        metricsQueueCF = pluginConfig.getSimpleValue("metricsQueueColumnFamily");
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void insertMetrics(MeasurementReport measurementReport) {
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);

        insertNumericData(keyspace, measurementReport.getNumericData(), measurementReport.getCollectionTime());
    }

    private void insertNumericData(Keyspace keyspace, Set<MeasurementDataNumeric> dataSet, long collectionTime) {
        int sevenDays = Duration.standardDays(7).toStandardSeconds().getSeconds();
        Set<Integer> scheduleIds = new TreeSet<Integer>();
        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());

        for (MeasurementDataNumeric data : dataSet) {
            scheduleIds.add(data.getScheduleId());
            mutator.addInsertion(data.getScheduleId(), rawMetricsDataCF, HFactory.createColumn(
                data.getTimestamp(), data.getValue(), sevenDays, LongSerializer.get(), DoubleSerializer.get()));
        }

        mutator.execute();

        updateMetricsQueue(keyspace, oneHourMetricsDataCF, collectionTime, scheduleIds);
    }

    private MutationResult updateMetricsQueue(Keyspace keyspace, String columnFamily, long collectionTime,
        Set<Integer> scheduleIds) {
        DateTime collectionHour = new DateTime(collectionTime).hourOfDay().roundFloorCopy();
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

        for (Integer scheduleId : scheduleIds) {
            Composite composite = new Composite();
            composite.addComponent(collectionHour.getMillis(), LongSerializer.get());
            composite.addComponent(scheduleId, IntegerSerializer.get());
            HColumn<Composite, Integer> column = HFactory.createColumn(composite, 0,
                CompositeSerializer.get(), IntegerSerializer.get());
            mutator.addInsertion(columnFamily, metricsQueueCF, column);
        }

        return mutator.execute();
    }

    @Override
    public void calculateAggregates() {
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);

        SliceQuery<String,Composite, Integer> metricsQueueQuery = HFactory.createSliceQuery(keyspace,
            StringSerializer.get(), new CompositeSerializer().get(), IntegerSerializer.get());
        metricsQueueQuery.setColumnFamily(metricsQueueCF);
        metricsQueueQuery.setKey(oneHourMetricsDataCF);

        ColumnSliceIterator<String, Composite, Integer> queueIterator =
            new ColumnSliceIterator<String, Composite, Integer>(metricsQueueQuery, (Composite) null, (Composite) null,
                false);

        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());

        while (queueIterator.hasNext()) {
            HColumn<Composite, Integer> column = queueIterator.next();
            Integer scheduleId = column.getName().get(1, IntegerSerializer.get());
            Long timestamp = column.getName().get(0, LongSerializer.get());
            DateTime startTime = new DateTime(timestamp);
            DateTime endTime = new DateTime(timestamp).plusHours(1);

            SliceQuery<Integer, Long, Double> rawMetricsQuery = HFactory.createSliceQuery(keyspace,
                IntegerSerializer.get(), LongSerializer.get(), DoubleSerializer.get());
            rawMetricsQuery.setColumnFamily(rawMetricsDataCF);
            rawMetricsQuery.setKey(scheduleId);

            ColumnSliceIterator<Integer, Long, Double> sliceIterator = new ColumnSliceIterator<Integer, Long, Double>(
                rawMetricsQuery, startTime.getMillis(), endTime.getMillis(), false);
            sliceIterator.hasNext();
            HColumn<Long, Double> rawColumn = sliceIterator.next();
            double min = rawColumn.getValue();
            double max = min;
            double sum = max;
            int count = 1;

            while (sliceIterator.hasNext()) {
                rawColumn = sliceIterator.next();
                if (rawColumn.getValue() < min) {
                    min = rawColumn.getValue();
                } else if (rawColumn.getValue() > max) {
                    max = rawColumn.getValue();
                }
                sum += rawColumn.getValue();
                ++count;
            }

            double avg = sum / count;

            mutator.addInsertion(scheduleId, oneHourMetricsDataCF, createAvgColumn(startTime, avg));
            mutator.addInsertion(scheduleId, oneHourMetricsDataCF, createMaxColumn(startTime, max));
            mutator.addInsertion(scheduleId, oneHourMetricsDataCF, createMinColumn(startTime, min));
        }

        mutator.execute();
    }

    private HColumn<Composite, Double> createAvgColumn(DateTime timestamp, double value) {
        return createAggregateColumn(AggregateType.AVG, timestamp, value);
    }

    private HColumn<Composite, Double> createMaxColumn(DateTime timestamp, double value) {
        return createAggregateColumn(AggregateType.MAX, timestamp, value);
    }

    private HColumn<Composite, Double> createMinColumn(DateTime timestamp, double value) {
        return createAggregateColumn(AggregateType.MIN, timestamp, value);
    }

    private HColumn<Composite, Double> createAggregateColumn(AggregateType type, DateTime timestamp, double value) {
        Composite composite = new Composite();
        composite.addComponent(timestamp.getMillis(), LongSerializer.get());
        composite.addComponent(type.ordinal(), IntegerSerializer.get());
        return HFactory.createColumn(composite, value);
    }

}
