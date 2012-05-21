package org.rhq.server.plugins.metrics.cassandra;

import java.util.Set;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.enterprise.server.plugin.pc.ServerPluginComponent;
import org.rhq.enterprise.server.plugin.pc.ServerPluginContext;
import org.rhq.enterprise.server.plugin.pc.metrics.MetricsServerPluginFacet;

import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * @author John Sanda
 */
public class CassandraMetricsPluginComponent implements MetricsServerPluginFacet, ServerPluginComponent {

    private Cluster cluster;

    private String keyspaceName;

    private String rawMetricsColumnFamily;

    @Override
    public void initialize(ServerPluginContext serverPluginContext) throws Exception {
        Configuration pluginConfig = serverPluginContext.getPluginConfiguration();

        cluster = HFactory.getOrCreateCluster(pluginConfig.getSimpleValue("clusterName"),
            pluginConfig.getSimpleValue("hostIP"));
        keyspaceName = pluginConfig.getSimpleValue("keyspace");
        rawMetricsColumnFamily = pluginConfig.getSimpleValue("rawMetricsColumnFamily");
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

        insertNumericData(keyspace, measurementReport.getNumericData());
    }

    private void insertNumericData(Keyspace keyspace, Set<MeasurementDataNumeric> dataSet) {
        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());

        for (MeasurementDataNumeric data : dataSet) {
            mutator.addInsertion(data.getScheduleId(), rawMetricsColumnFamily, HFactory.createColumn(
                data.getTimestamp(), data.getValue()));
        }

        mutator.execute();
    }
}
