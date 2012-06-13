package org.rhq.server.plugins.metrics.cassandra;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeFieldType;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.chrono.GregorianChronology;
import org.joda.time.field.DividedDateTimeField;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.criteria.MeasurementDataTraitCriteria;
import org.rhq.core.domain.measurement.MeasurementDataNumeric;
import org.rhq.core.domain.measurement.MeasurementReport;
import org.rhq.core.domain.measurement.TraitMeasurement;
import org.rhq.core.domain.util.PageList;
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

    private String traitsCF;

    private String resourceTraitsCF;

    static final int SEVEN_DAYS = Duration.standardDays(7).toStandardSeconds().getSeconds();

    static final int TWO_WEEKS = Duration.standardDays(14).toStandardSeconds().getSeconds();

    static final int ONE_MONTH = Duration.standardDays(31).toStandardSeconds().getSeconds();

    static final int ONE_YEAR = Duration.standardDays(365).toStandardSeconds().getSeconds();

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
        traitsCF = pluginConfig.getSimpleValue("traitsColumnFamily");
        resourceTraitsCF = pluginConfig.getSimpleValue("resourceTraitsColumnFamily");
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
    public PageList<? extends TraitMeasurement> findTraitsByCriteria(Subject subject,
        MeasurementDataTraitCriteria measurementDataTraitCriteria) {
        return null;
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
            mutator.addInsertion(data.getScheduleId(), rawMetricsDataCF, HFactory.createColumn(
                data.getTimestamp(), data.getValue(), SEVEN_DAYS, LongSerializer.get(), DoubleSerializer.get()));
        }

        mutator.execute();

        updateMetricsQueue(keyspace, oneHourMetricsDataCF, collectionTime, scheduleIds);
    }

    @Override
    public void calculateAggregates() {
        Keyspace keyspace = HFactory.createKeyspace(keyspaceName, cluster);

        Map<Integer, DateTime> updatedSchedules = aggregateRawData(keyspace);
        updateMetricsQueue(keyspace, sixHourMetricsDataCF, updatedSchedules);

        updatedSchedules = calculateAggregates(keyspace, oneHourMetricsDataCF, sixHourMetricsDataCF,
            Minutes.minutes(60 * 6), ONE_MONTH);
        updateMetricsQueue(keyspace, twentyFourHourMetricsDataCF, updatedSchedules);

        calculateAggregates(keyspace, sixHourMetricsDataCF, twentyFourHourMetricsDataCF,
            Hours.hours(24).toStandardMinutes(), ONE_YEAR);
    }

    private Map<Integer, DateTime> aggregateRawData(Keyspace keyspace) {
        Map<Integer, DateTime> updatedSchedules = new TreeMap<Integer, DateTime>();

        SliceQuery<String,Composite, Integer> queueQuery = HFactory.createSliceQuery(keyspace,
            StringSerializer.get(), new CompositeSerializer().get(), IntegerSerializer.get());
        queueQuery.setColumnFamily(metricsQueueCF);
        queueQuery.setKey(oneHourMetricsDataCF);

        ColumnSliceIterator<String, Composite, Integer> queueIterator =
            new ColumnSliceIterator<String, Composite, Integer>(queueQuery, (Composite) null, (Composite) null,
                false);

        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());

        while (queueIterator.hasNext()) {
            HColumn<Composite, Integer> queueColumn = queueIterator.next();
            Integer scheduleId = queueColumn.getName().get(1, IntegerSerializer.get());
            Long timestamp = queueColumn.getName().get(0, LongSerializer.get());
            DateTime startTime = new DateTime(timestamp);
            DateTime endTime = new DateTime(timestamp).plus(Minutes.minutes(60));

            SliceQuery<Integer, Long, Double> rawDataQuery = HFactory.createSliceQuery(keyspace,
                IntegerSerializer.get(), LongSerializer.get(), DoubleSerializer.get());
            rawDataQuery.setColumnFamily(rawMetricsDataCF);
            rawDataQuery.setKey(scheduleId);

            ColumnSliceIterator<Integer, Long, Double> rawDataIterator =
                new ColumnSliceIterator<Integer, Long, Double>(rawDataQuery, startTime.getMillis(), endTime.getMillis(),
                    false);
            rawDataIterator.hasNext();

            HColumn<Long, Double> rawDataColumn = rawDataIterator.next();
            double min = rawDataColumn.getValue();
            double max = min;
            double sum = max;
            int count = 1;

            while (rawDataIterator.hasNext()) {
                rawDataColumn = rawDataIterator.next();
                if (rawDataColumn.getValue() < min) {
                    min = rawDataColumn.getValue();
                } else if (rawDataColumn.getValue() > max) {
                    max = rawDataColumn.getValue();
                }
                sum += rawDataColumn.getValue();
                ++count;
            }

            double avg = sum / count;

            mutator.addInsertion(scheduleId, oneHourMetricsDataCF, createAvgColumn(startTime, avg, TWO_WEEKS));
            mutator.addInsertion(scheduleId, oneHourMetricsDataCF, createMaxColumn(startTime, max, TWO_WEEKS));
            mutator.addInsertion(scheduleId, oneHourMetricsDataCF, createMinColumn(startTime, min, TWO_WEEKS));

            updatedSchedules.put(scheduleId, startTime);

            queueMutator.addDeletion(oneHourMetricsDataCF, metricsQueueCF, queueColumn.getName(),
                CompositeSerializer.get());
        }
        mutator.execute();
        queueMutator.execute();

        return updatedSchedules;
    }

    private Map<Integer, DateTime> calculateAggregates(Keyspace keyspace, String fromColumnFamily,
        String toColumnFamily, Minutes interval, int ttl) {
        DateTime currentHour = getCurrentHour();
        DateTimeComparator dateTimeComparator = DateTimeComparator.getInstance();

        Map<Integer, DateTime> updatedSchedules = new TreeMap<Integer, DateTime>();

        SliceQuery<String,Composite, Integer> queueQuery = HFactory.createSliceQuery(keyspace,
            StringSerializer.get(), new CompositeSerializer().get(), IntegerSerializer.get());
        queueQuery.setColumnFamily(metricsQueueCF);
        queueQuery.setKey(toColumnFamily);

        ColumnSliceIterator<String, Composite, Integer> queueIterator =
            new ColumnSliceIterator<String, Composite, Integer>(queueQuery, (Composite) null, (Composite) null,
                false);

        Mutator<Integer> mutator = HFactory.createMutator(keyspace, IntegerSerializer.get());
        Mutator<String> queueMutator = HFactory.createMutator(keyspace, StringSerializer.get());

        while (queueIterator.hasNext()) {
            HColumn<Composite, Integer> queueColumn = queueIterator.next();
            Integer scheduleId = queueColumn.getName().get(1, IntegerSerializer.get());
            Long timestamp = queueColumn.getName().get(0, LongSerializer.get());
            DateTime startTime = new DateTime(timestamp);
            DateTime endTime = new DateTime(timestamp).plus(interval);

            if (dateTimeComparator.compare(currentHour, endTime) < 0) {
                continue;
            }

            Composite startColKey = new Composite();
            startColKey.addComponent(startTime.getMillis(), LongSerializer.get());

            Composite endColKey = new Composite();
            endColKey.addComponent(endTime.getMillis(), LongSerializer.get());

            SliceQuery<Integer, Composite, Double> fromColumnFamilyQuery = HFactory.createSliceQuery(keyspace,
                IntegerSerializer.get(), CompositeSerializer.get(), DoubleSerializer.get());
            fromColumnFamilyQuery.setColumnFamily(fromColumnFamily);
            fromColumnFamilyQuery.setKey(scheduleId);

            ColumnSliceIterator<Integer, Composite, Double> fromColumnFamilyIterator =
                new ColumnSliceIterator<Integer, Composite, Double>(fromColumnFamilyQuery, startColKey, endColKey,
                    false);
            fromColumnFamilyIterator.hasNext();

            HColumn<Composite, Double> fromColumn = fromColumnFamilyIterator.next();
            double min = 0;
            double max = 0;
            double sum = 0;
            int avgCount = 0;
            int minCount = 0;
            int maxCount = 0;

            while (fromColumnFamilyIterator.hasNext()) {
                fromColumn = fromColumnFamilyIterator.next();
                AggregateType type = AggregateType.valueOf(fromColumn.getName().get(1, IntegerSerializer.get()));

                switch (type) {
                    case AVG:
                        sum += fromColumn.getValue();
                        avgCount++;
                        break;
                    case MIN:
                        if (minCount == 0) {
                            min = fromColumn.getValue();
                        } else if (min < fromColumn.getValue()) {
                            min = fromColumn.getValue();
                        }
                        break;
                    case MAX:
                        if (maxCount == 0) {
                            max = fromColumn.getValue();
                        } else if (max > fromColumn.getValue()) {
                            max = fromColumn.getValue();
                        }
                        break;
                }
            }

            double avg = sum / avgCount;

            mutator.addInsertion(scheduleId, toColumnFamily, createAvgColumn(startTime, avg, ttl));
            mutator.addInsertion(scheduleId, toColumnFamily, createMaxColumn(startTime, max, ttl));
            mutator.addInsertion(scheduleId, toColumnFamily, createMinColumn(startTime, min, ttl));

            updatedSchedules.put(scheduleId, startTime);

            queueMutator.addDeletion(toColumnFamily, metricsQueueCF, queueColumn.getName(), CompositeSerializer.get());
        }
        mutator.execute();
        queueMutator.execute();

        return updatedSchedules;
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

    private MutationResult updateMetricsQueue(Keyspace keyspace, String columnFamily,
        Map<Integer, DateTime> updatedSchedules) {
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

        for (Integer scheduleId : updatedSchedules.keySet()) {
            Composite composite = new Composite();
            DateTime timeSlice = getTimeSlice(columnFamily, updatedSchedules.get(scheduleId));
            composite.addComponent(timeSlice.getMillis(), LongSerializer.get());
            composite.addComponent(scheduleId, IntegerSerializer.get());
            HColumn<Composite, Integer> column = HFactory.createColumn(composite, 0, CompositeSerializer.get(),
                IntegerSerializer.get());
            mutator.addInsertion(columnFamily, metricsQueueCF, column);
        }

        return mutator.execute();
    }

    private DateTime getTimeSlice(String columnFamily, DateTime dateTime) {
        if (columnFamily.equals(oneHourMetricsDataCF)) {
            return dateTime.hourOfDay().roundFloorCopy();
        } else if (columnFamily.equals(sixHourMetricsDataCF)) {
            return get6HourTimeSlice(dateTime);
        } else if (columnFamily.equals(twentyFourHourMetricsDataCF)) {
            return get24HourTimeSlice(dateTime);
        } else {
            throw new IllegalArgumentException(columnFamily + " is not yet supported");
        }
    }

    private DateTime get6HourTimeSlice(DateTime dateTime) {
        Chronology chronology = GregorianChronology.getInstance();
        DateTimeField hourField = chronology.hourOfDay();
        DividedDateTimeField dividedField = new DividedDateTimeField(hourField, DateTimeFieldType.clockhourOfDay(), 6);
        long timestamp = dividedField.roundFloor(dateTime.getMillis());

        return new DateTime(timestamp);
    }

    private DateTime get24HourTimeSlice(DateTime dateTime) {
        Chronology chronology = GregorianChronology.getInstance();
        DateTimeField hourField = chronology.hourOfDay();
        DividedDateTimeField dividedField = new DividedDateTimeField(hourField, DateTimeFieldType.clockhourOfDay(), 24);
        long timestamp = dividedField.roundFloor(dateTime.getMillis());

        return new DateTime(timestamp);
    }

    private HColumn<Composite, Double> createAvgColumn(DateTime timestamp, double value, int ttl) {
        return createAggregateColumn(AggregateType.AVG, timestamp, value, ttl);
    }

    private HColumn<Composite, Double> createMaxColumn(DateTime timestamp, double value, int ttl) {
        return createAggregateColumn(AggregateType.MAX, timestamp, value, ttl);
    }

    private HColumn<Composite, Double> createMinColumn(DateTime timestamp, double value, int ttl) {
        return createAggregateColumn(AggregateType.MIN, timestamp, value, ttl);
    }

    private HColumn<Composite, Double> createAggregateColumn(AggregateType type, DateTime timestamp, double value,
        int ttl) {
        Composite composite = new Composite();
        composite.addComponent(timestamp.getMillis(), LongSerializer.get());
        composite.addComponent(type.ordinal(), IntegerSerializer.get());
        return HFactory.createColumn(composite, value, ttl, CompositeSerializer.get(), DoubleSerializer.get());
    }

    protected DateTime getCurrentHour() {
        DateTime now = new DateTime();
        return now.hourOfDay().roundFloorCopy();
    }

}
