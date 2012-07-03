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
import org.rhq.core.domain.criteria.ResourceCriteria;
import org.rhq.core.domain.criteria.ResourceGroupCriteria;
import org.rhq.core.domain.resource.Resource;
import org.rhq.core.domain.resource.ResourceCategory;
import org.rhq.core.domain.resource.group.ResourceGroup;
import org.rhq.core.domain.util.PageList;
import org.rhq.core.util.stream.StreamUtil;
import org.rhq.enterprise.server.auth.SubjectManagerLocal;
import org.rhq.enterprise.server.bundle.BundleManagerLocal;
import org.rhq.enterprise.server.plugin.pc.ControlResults;
import org.rhq.enterprise.server.resource.ResourceManagerLocal;
import org.rhq.enterprise.server.resource.ResourceNotFoundException;
import org.rhq.enterprise.server.resource.group.ResourceGroupManagerLocal;
import org.rhq.enterprise.server.util.LookupUtil;

/**
 * @author John Sanda
 */
public class OperationsDelegate {

    private ResourceGroupManagerLocal resourceGroupManager;

    private BundleManagerLocal bundleManager;

    private Subject overlord;

    public OperationsDelegate() {
        SubjectManagerLocal subjectManager = LookupUtil.getSubjectManager();
        overlord = subjectManager.getOverlord();
    }

    public ControlResults invoke(String operation, Configuration params) {
        ControlResults results = new ControlResults();

        try {
            BundleManagerLocal bundleManager = LookupUtil.getBundleManager();

            BundleType bundleType = bundleManager.getBundleType(overlord, "Ant Bundle");
            Bundle bundle = bundleManager.createBundle(overlord, "Cassandra Dev Node Bundle",
                "Cassandra Dev Node Bundle", bundleType.getId());

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            StreamUtil.copy(getClass().getResourceAsStream("cassandra-bundle.jar"), outputStream);

            BundleVersion bundleVersion = bundleManager.createBundleVersionViaByteArray(overlord,
                outputStream.toByteArray());

            Configuration bundleConfig = new Configuration();
            File clusterDir = new File(params.getSimpleValue("clusterDirectory"));
            int numNodes = Integer.parseInt(params.getSimpleValue("numberOfNodes"));
            int replicationFactor = Integer.parseInt(params.getSimpleValue("replicationFactor", "1"));
            String hostname = params.getSimpleValue("host");

            Resource platform = getPlatform(hostname);
            ResourceGroup group = getPlatformGroup(platform, hostname);

            Set<String> ipAddresses = calculateLocalIPAddresses(numNodes);

            for (int i = 0; i < numNodes; ++i) {
                Set<String> seeds = getSeeds(ipAddresses, i + 1);
                int jmxPort = 7200 + i;

                Configuration deploymentConfig = new Configuration();
                deploymentConfig.put(new PropertySimple("cluster.name", "rhqdev"));
                deploymentConfig.put(new PropertySimple("cluster.dir", clusterDir.getAbsolutePath()));
                deploymentConfig.put(new PropertySimple("auto.bootstrap", "false"));
                deploymentConfig.put(new PropertySimple("data.dir", "data"));
                deploymentConfig.put(new PropertySimple("commitlog.dir", "commit_log"));
                deploymentConfig.put(new PropertySimple("log.dir", "logs"));
                deploymentConfig.put(new PropertySimple("saved.caches.dir", "saved_caches"));
                deploymentConfig.put(new PropertySimple("hostname", getLocalIPAddress(i + 1)));
                deploymentConfig.put(new PropertySimple("seeds", collectionToString(seeds)));
                deploymentConfig.put(new PropertySimple("jmx.port", Integer.toString(jmxPort)));
                deploymentConfig.put(new PropertySimple("initial.token", generateToken(i, numNodes)));
                deploymentConfig.put(new PropertySimple("install.schema", i == 0));
                deploymentConfig.put(new PropertySimple("replication.factor", replicationFactor));

                String destinationName = "cassandra-node[" + i + "]-deployment";

                BundleDestination bundleDestination = bundleManager.createBundleDestination(overlord, bundle.getId(),
                    destinationName, destinationName, "Root File System",
                    new File(clusterDir, "node" + i).getAbsolutePath(), group.getId());

                BundleDeployment bundleDeployment = bundleManager.createBundleDeployment(overlord, bundleVersion.getId(),
                    bundleDestination.getId(), destinationName, deploymentConfig);

                bundleManager.scheduleBundleDeployment(overlord, bundleDeployment.getId(), false);
            }

            return new ControlResults();
        } catch (ResourceNotFoundException e) {
            results.setError(e.getMessage());
            return results;

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

    private Resource getPlatform(String hostname) {
        ResourceCriteria criteria = new ResourceCriteria();
        criteria.setFiltersOptional(true);
        criteria.addFilterResourceKey(hostname);
        criteria.addFilterName(hostname);
        criteria.addFilterResourceCategories(ResourceCategory.PLATFORM);
        criteria.fetchResourceType(true);
        criteria.fetchExplicitGroups(true);

        ResourceManagerLocal resourceManager = LookupUtil.getResourceManager();
        PageList<Resource> resources = resourceManager.findResourcesByCriteria(overlord, criteria);

        if (resources.isEmpty()) {
            String msg = "Could not find platform with hostname " + hostname + ". The value that you specify for the " +
                "host argument should match either a platform's resource name and/or its resource key.";
            throw new ResourceNotFoundException(msg);
        }

        return resources.get(0);
    }

    private ResourceGroup getPlatformGroup(Resource platform, String hostname) {
        String groupName = hostname + " [Local Cassandra Cluster]";

        ResourceGroupCriteria criteria = new ResourceGroupCriteria();
        criteria.addFilterExplicitResourceCategory(ResourceCategory.PLATFORM);
        criteria.addFilterName(groupName);

        PageList<ResourceGroup> groups = getResourceGroupManager().findResourceGroupsByCriteria(overlord, criteria);

        if (groups.isEmpty()) {
            return createPlatformGroup(groupName, platform);
        }

        return groups.get(0);
    }

    private ResourceGroup createPlatformGroup(String groupName, Resource resource) {
        ResourceGroup group = new ResourceGroup(groupName, resource.getResourceType());
        group.addExplicitResource(resource);

        return getResourceGroupManager().createResourceGroup(overlord, group);
    }

//    private BundleManagerLocal getBundleManager() {
//
//    }

    private ResourceGroupManagerLocal getResourceGroupManager() {
        if (resourceGroupManager == null) {
            resourceGroupManager = LookupUtil.getResourceGroupManager();
        }
        return resourceGroupManager;
    }
}

