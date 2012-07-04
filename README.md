# Cassandra Metrics RHQ Server Plugin

This is an [RHQ](http://www.jboss.org/rhq) server plugin that uses [Cassandra](http://cassandra.apache.org)
for persisting and managing metric data. This plugin is part of a prototyping effort to
evaluate different solutions as a next-generate data store for metrics, event logging, and
other data performance and scalability demands that cannot always be handled by a 
traditional RDBMS.

## Build Instructions
Follow the steps for [building RHQ](https://docs.jboss.org/author/display/RHQ/Building+RHQ).

In your RHQ git repo check out the remote branch jsanda/metrics-rhq:

    $ git checkout --track -b jsanda/metrics-rhq origin/ origin/jsanda/metrics-rhq

The jsanda/metrics-rhq branch servers as the upstream for the Cassandra plugin. The 
metrics-rhq branch may also serve as the upstream for other metrics servers that use a
different back end.

Build the jsanda/metrics-rhq branch (from the top of the RHQ source tree):

    $ mvn clean install -DskipTests -Ddbreset -Ddb=dev -Pdev,metrics

Now build the source for this project:

    $ mvn clean install -DskipTests -Pdev

## Running Unit/Integration Tests
Most of the tests require an instance of Cassandra running locally. They also require the 
RHQ schema to be installed. Tests can be run in a few different ways. 

### Manual Set Up
You can install and configure Cassandra manually and use that set up with,

    $ mvn clean test

### Automated Set Up with Single Node
This approach will install and configure a single node.

    $ mvn clean test -Dcluster.mode=single

The RHQ schema is installed prior to tests executing, and the Cassandra node shuts down
after tests finish running.

### Automated Set Up with Multiple Nodes
This approach will install and configure multiple nodes.

    $ mvn clean test -Dcluster.mode=multi -Dreplication.factor=2

By default the cluster will contain 4 nodes. The number of nodes is configurable by setting
the `cassandra.cluster.size` property. Also note in the above comamnd line that the
`replication.factor` property is set to a value of 2. It has a default of 1. You may want
to adjust this based on your cluster size.
