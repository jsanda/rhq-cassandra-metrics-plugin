package org.rhq.server.plugins.metrics.cassandra;

import static org.rhq.core.util.StringUtil.collectionToString;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.rhq.core.domain.auth.Subject;
import org.rhq.core.domain.bundle.Bundle;
import org.rhq.core.domain.bundle.BundleDeployment;
import org.rhq.core.domain.bundle.BundleDestination;
import org.rhq.core.domain.bundle.BundleType;
import org.rhq.core.domain.bundle.BundleVersion;
import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.domain.configuration.PropertySimple;
import org.rhq.core.domain.criteria.ResourceGroupCriteria;
import org.rhq.core.domain.resource.ResourceCategory;
import org.rhq.core.domain.resource.group.ResourceGroup;
import org.rhq.core.domain.util.PageList;
import org.rhq.core.util.stream.StreamUtil;
import org.rhq.enterprise.server.auth.SubjectManagerLocal;
import org.rhq.enterprise.server.bundle.BundleManagerLocal;
import org.rhq.enterprise.server.plugin.pc.ControlResults;
import org.rhq.enterprise.server.resource.group.ResourceGroupManagerLocal;
import org.rhq.enterprise.server.util.LookupUtil;

/**
 * @author John Sanda
 */
public class OperationsDelegate {

    public ControlResults invoke(String operation, Configuration params) {
        try {
            SubjectManagerLocal subjectManager = LookupUtil.getSubjectManager();
            Subject overlord = subjectManager.getOverlord();

            BundleManagerLocal bundleManager = LookupUtil.getBundleManager();

            BundleType bundleType = bundleManager.getBundleType(overlord, "Ant Bundle");
            Bundle bundle = bundleManager.createBundle(overlord, "Cassandra Dev Node Bundle",
                "Cassandra Dev Node Bundle", bundleType.getId());

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            StreamUtil.copy(getClass().getResourceAsStream("cassandra-bundle.zip"), outputStream);

            BundleVersion bundleVersion = bundleManager.createBundleVersionViaByteArray(overlord,
                outputStream.toByteArray());

            Configuration bundleConfig = new Configuration();
            File clusterDir = new File(params.getSimpleValue("clusterDirectory"));
            int numNodes = Integer.parseInt(params.getSimpleValue("numberOfNodes"));

            ResourceGroup group = findPlatformGroup("Cassandra Hosts");

            Set<String> ipAddresses = calculateLocalIPAddresses(numNodes);

            for (int i = 0; i < numNodes; ++i) {
                Set<String> seeds = getSeeds(ipAddresses, i + 1);
                int jmxPort = 7200 + i;

                Configuration deploymentConfig = new Configuration();
                deploymentConfig.put(new PropertySimple("cluster.name", "rhqdev"));
                deploymentConfig.put(new PropertySimple("cluster.dir", clusterDir.getAbsolutePath()));
                deploymentConfig.put(new PropertySimple("auto.bootstrap", "false"));
                deploymentConfig.put(new PropertySimple("data.dir", "data"));
                deploymentConfig.put(new PropertySimple("commitlog.dir", "log"));
                deploymentConfig.put(new PropertySimple("saved.caches.dir", "saved_caches"));
                deploymentConfig.put(new PropertySimple("hostname", getLocalIPAddress(i + 1)));
                deploymentConfig.put(new PropertySimple("seeds", collectionToString(seeds)));
                deploymentConfig.put(new PropertySimple("jmx.port", Integer.toString(jmxPort)));
                deploymentConfig.put(new PropertySimple("initial.token", generateToken(i, numNodes)));
                deploymentConfig.put(new PropertySimple("install.schema", i == 0));

                String destinationName = "cassandra-node[" + i + "]-deployment";

                BundleDestination bundleDestination = bundleManager.createBundleDestination(overlord, bundle.getId(),
                    destinationName, destinationName, "Root File System",
                    new File(clusterDir, "node" + i).getAbsolutePath(), group.getId());

                BundleDeployment bundleDeployment = bundleManager.createBundleDeployment(overlord, bundleVersion.getId(),
                    bundleDestination.getId(), destinationName, deploymentConfig);

                bundleManager.scheduleBundleDeployment(overlord, bundleDeployment.getId(), false);
            }

            return new ControlResults();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Set<String> calculateLocalIPAddresses(int numNodes) {
        Set<String> addresses = new HashSet<String>();
        for (int i = 1; i <= numNodes; ++i) {
            addresses.add(getLocalIPAddress(i));
        }
        return addresses;
    }

    private Set<String> getSeeds(Set<String> addresses, int i) {
        Set<String> seeds = new HashSet<String>();
        String address = getLocalIPAddress(i);

        for (String nodeAddress : addresses) {
            if (nodeAddress.equals(address)) {
                continue;
            } else {
                seeds.add(nodeAddress);
            }
        }

        return seeds;
    }

    private String getLocalIPAddress(int i) {
        return "127.0.0." + i;
    }

    private String generateToken(int i, int numNodes) {
        BigInteger num = new BigInteger("2").pow(127).divide(new BigInteger(Integer.toString(numNodes)));
        return num.multiply(new BigInteger(Integer.toString(i))).toString();
    }

    private ResourceGroup findPlatformGroup(String groupName) {
        SubjectManagerLocal subjectManager = LookupUtil.getSubjectManager();
        Subject overlord = subjectManager.getOverlord();

        ResourceGroupCriteria criteria = new ResourceGroupCriteria();
        criteria.addFilterExplicitResourceCategory(ResourceCategory.PLATFORM);
        criteria.addFilterName(groupName);

        ResourceGroupManagerLocal groupManager = LookupUtil.getResourceGroupManager();
        PageList<ResourceGroup> groups = groupManager.findResourceGroupsByCriteria(overlord, criteria);

        if (groups.isEmpty()) {
            throw new IllegalArgumentException("No platform group with name <" + groupName + "> found.");
        }

        return groups.get(0);
    }
}

