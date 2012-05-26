package org.rhq.server.plugins.metrics.cassandra;

import java.util.Set;
import java.util.TreeSet;

import org.joda.time.DateTime;

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
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * @author John Sanda
 */
public class CassandraMetricsPluginComponent implements MetricsServerPluginFacet, ServerPluginComponent {

    private Cluster cluster;

    private String keyspaceName;

    private String rawMetricsColumnFamily;

    private String metricsQueueColumnFamily;

    @Override
    public void initialize(ServerPluginContext serverPluginContext) throws Exception {
        Configuration pluginConfig = serverPluginContext.getPluginConfiguration();

        cluster = HFactory.getOrCreateCluster(pluginConfig.getSimpleValue("clusterName"),
            pluginConfig.getSimpleValue("hostIP"));
        keyspaceName = pluginConfig.getSimpleValue("keyspace");
        rawMetricsColumnFamily = pluginConfig.getSimpleValue("rawMetricsColumnFamily");
        metricsQueueColumnFamily = pluginConfig.getSimpleValue("metricsQueueColumnFamily");
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
        Set<Integer> scheduleIds = new TreeSet<Integer>();
        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());

        for (MeasurementDataNumeric data : dataSet) {
            scheduleIds.add(data.getScheduleId());
            mutator.addInsertion(data.getScheduleId(), rawMetricsColumnFamily, HFactory.createColumn(
                data.getTimestamp(), data.getValue(), 30, LongSerializer.get(), DoubleSerializer.get()));
        }

        mutator.execute();

        updateMetricsQueue(keyspace, collectionTime, scheduleIds);
    }

    private MutationResult updateMetricsQueue(Keyspace keyspace, long collectionTime, Set<Integer> scheduleIds) {
        DateTime collectionHour = new DateTime(collectionTime).hourOfDay().roundFloorCopy();
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());

        for (Integer scheduleId : scheduleIds) {
            Composite composite = new Composite();
            composite.addComponent(collectionHour.getMillis(), LongSerializer.get());
            composite.addComponent(scheduleId, IntegerSerializer.get());
            HColumn<Composite, Integer> column = HFactory.createColumn(composite, 0,
                CompositeSerializer.get(), IntegerSerializer.get());
            queueMutator.addInsertion(rawMetricsColumnFamily, metricsQueueColumnFamily, column);
        }

        return queueMutator.execute();
    }

    @Override
    public void calculateAggregates() {

    }
}
