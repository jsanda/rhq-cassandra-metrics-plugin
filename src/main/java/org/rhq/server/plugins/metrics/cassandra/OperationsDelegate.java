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
import org.rhq.core.domain.criteria.BundleCriteria;
import org.rhq.core.domain.criteria.BundleDestinationCriteria;
import org.rhq.core.domain.criteria.BundleVersionCriteria;
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

    // Note that this needs to match the value in deploy.xml
    private String bundleName = "Cassandra Dev Node Bundle";

    // Note that this needs to match the value in deploy.xml
    private String bundleVersion = "1.0";

    public OperationsDelegate() {
        SubjectManagerLocal subjectManager = LookupUtil.getSubjectManager();
        overlord = subjectManager.getOverlord();

        resourceGroupManager = LookupUtil.getResourceGroupManager();
        bundleManager = LookupUtil.getBundleManager();
    }

    public ControlResults invoke(String operation, Configuration params) {
        ControlResults results = new ControlResults();

        try {
            Bundle bundle = getBundle();
            BundleVersion bundleVersion = getBundleVersion(bundle);

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
                String deployDir = new File(clusterDir, "node" + i).getAbsolutePath();

                BundleDestination bundleDestination = getBundleDestination(bundleVersion, destinationName, group,
                    deployDir);

                BundleDeployment bundleDeployment = bundleManager.createBundleDeployment(overlord, bundleVersion.getId(),
                    bundleDestination.getId(), destinationName, deploymentConfig);

                bundleManager.scheduleBundleDeployment(overlord, bundleDeployment.getId(), true);
            }

            return new ControlResults();
        } catch (ResourceNotFoundException e) {
            results.setError(e.getMessage());
            return results;

        } catch (Exception e) {
            results.setError(e);
            return results;
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

    private Bundle getBundle() throws Exception {
        BundleType bundleType = bundleManager.getBundleType(overlord, "Ant Bundle");

        BundleCriteria criteria = new BundleCriteria();
        criteria.addFilterBundleTypeName(bundleType.getName());
        criteria.addFilterName(bundleName);

        PageList<Bundle> bundles = bundleManager.findBundlesByCriteria(overlord, criteria);

        if (bundles.isEmpty()) {
            return bundleManager.createBundle(overlord, bundleName, bundleName, bundleType.getId());
        }

        return bundles.get(0);
    }

    private BundleVersion getBundleVersion(Bundle bundle) throws Exception {
        BundleVersionCriteria criteria = new BundleVersionCriteria();
        criteria.addFilterBundleId(bundle.getId());
        criteria.addFilterVersion(bundleVersion);
        criteria.fetchBundle(true);
        criteria.fetchBundleDeployments(true);

        PageList<BundleVersion> bundleVersions = bundleManager.findBundleVersionsByCriteria(overlord, criteria);

        if (bundleVersions.isEmpty()) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            StreamUtil.copy(getClass().getResourceAsStream("cassandra-bundle.jar"), outputStream);
            return bundleManager.createBundleVersionViaByteArray(overlord, outputStream.toByteArray());
        }

        return bundleVersions.get(0);
    }

    private BundleDestination getBundleDestination(BundleVersion bundleVersion, String destinationName,
        ResourceGroup group, String deployDir) throws Exception {
        BundleDestinationCriteria criteria = new BundleDestinationCriteria();
        criteria.addFilterBundleId(bundleVersion.getBundle().getId());
        //criteria.addFilterBundleVersionId(bundleVersion.getId());
        criteria.addFilterGroupId(group.getId());

        PageList<BundleDestination> bundleDestinations = bundleManager.findBundleDestinationsByCriteria(overlord,
            criteria);

        if (bundleDestinations.isEmpty()) {
            return bundleManager.createBundleDestination(overlord, bundleVersion.getBundle().getId(),
                destinationName, destinationName, "Root File System", deployDir, group.getId());
        }

        for (BundleDestination destination : bundleDestinations) {
            if (destination.getDeployDir().equals(deployDir)) {
                return destination;
            }
        }

        throw new RuntimeException("Unable to get bundle destination for [bundleId: " +
            bundleVersion.getBundle().getId() + ", bunldleVersionId: " + bundleVersion.getId() + ", destination: " +
            destinationName + ", deployDir: " + deployDir + "]");
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

        PageList<ResourceGroup> groups = resourceGroupManager.findResourceGroupsByCriteria(overlord, criteria);

        if (groups.isEmpty()) {
            return createPlatformGroup(groupName, platform);
        }

        return groups.get(0);
    }

    private ResourceGroup createPlatformGroup(String groupName, Resource resource) {
        ResourceGroup group = new ResourceGroup(groupName, resource.getResourceType());
        group.addExplicitResource(resource);

        return resourceGroupManager.createResourceGroup(overlord, group);
    }

}

