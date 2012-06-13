package org.rhq.server.plugins.metrics.cassandra;

import me.prettyprint.cassandra.service.KeyIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;

/**
 * @author John Sanda
 */
public class DAO {

    private Keyspace keyspace;

    public DAO(Keyspace keyspace) {
        this.keyspace = keyspace;
    }

    public <K> MutationResult deleteAllRows(String columnFamily, Serializer<K> keySerializer) {
        KeyIterator<K> keyIterator = new KeyIterator<K>(keyspace, columnFamily, keySerializer);
        Mutator<K> rowMutator = HFactory.createMutator(keyspace, keySerializer);
        rowMutator.addDeletion(keyIterator, columnFamily);

        return rowMutator.execute();
    }

}
