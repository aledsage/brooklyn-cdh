package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.JavaGroovyEquivalents.groovyTruth;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.jclouds.compute.domain.OsFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.BasicConfigurableEntityFactory;
import brooklyn.entity.basic.ConfigurableEntityFactory;
import brooklyn.entity.basic.Description;
import brooklyn.entity.basic.NamedParameter;
import brooklyn.entity.basic.SoftwareProcessImpl;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.basic.lifecycle.ScriptRunner;
import brooklyn.event.feed.function.FunctionFeed;
import brooklyn.event.feed.function.FunctionPollConfig;
import brooklyn.location.MachineProvisioningLocation;
import brooklyn.location.jclouds.JcloudsLocationConfig;
import brooklyn.location.jclouds.templates.PortableTemplateBuilder;
import brooklyn.util.MutableMap;
import brooklyn.util.MutableSet;

import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ClouderaCdhNodeImpl extends SoftwareProcessImpl implements ClouderaCdhNode {

    private static final Logger log = LoggerFactory.getLogger(ClouderaCdhNodeImpl.class);
    
    public static ConfigurableEntityFactory<ClouderaCdhNodeImpl> newFactory() { 
        return new BasicConfigurableEntityFactory<ClouderaCdhNodeImpl>(ClouderaCdhNodeImpl.class);
    }
    
    public ClouderaCdhNodeImpl() {
    }

    public ClouderaCdhNodeImpl(Map flags) {
    	super(flags);
    }

    public ClouderaCdhNodeImpl(Entity parent) {
    	super(parent);
    }

    public ClouderaCdhNodeImpl(Map flags, Entity parent) {
    	super(flags, parent);
    }
    

    @Override
    public Class getDriverInterface() {
    	return ClouderaCdhNodeSshDriver.class;
	}
    
    @Override
    public ClouderaCdhNodeDriver getDriver() {
    	return (ClouderaCdhNodeDriver) super.getDriver();
    }
    
    protected Map<String,Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        Map flags = super.obtainProvisioningFlags(location);
        flags.put("templateBuilder", new PortableTemplateBuilder()
	            .osFamily(OsFamily.UBUNTU).osVersionMatches("12.04")
	            .os64Bit(true)
	            .minRam(2560));
        flags.put(JcloudsLocationConfig.SECURITY_GROUPS.getName(), "universal");
        return flags;
    }

    // 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030
    protected Collection<Integer> getRequiredOpenPorts() {
        return MutableSet.<Integer>builder()
        		.addAll(22, 2181, 7180, 7182, 8088, 8888, 50030, 50060, 50070, 50090, 60010, 60020, 60030)
        		.addAll(super.getRequiredOpenPorts())
				.build();
    }
    
    public void connectSensors() {
        super.connectSensors();
        /*
         FunctionSensorAdapter fnSensorAdaptor = sensorRegistry.register(new FunctionSensorAdapter({}, period: 30*TimeUnit.SECONDS));
         def mgdh = fnSensorAdaptor.then { getManagedHostId() };
         mgdh.poll(CDH_HOST_ID);
         mgdh.poll(SERVICE_UP, { it!=null });
         */

        FunctionFeed feed = FunctionFeed.builder()
                .entity(this)
                .poll(new FunctionPollConfig<Boolean,Boolean>(SERVICE_UP)
		                .period(30, TimeUnit.SECONDS)
		                .callable(new Callable<Boolean>() {
		                    @Override
		                    public Boolean call() throws Exception {
		                    	return getManagedHostId()!=null;
		                    }
		                })
		                .onError(Functions.constant(false))
		                )
                .poll(new FunctionPollConfig<String,String>(CDH_HOST_ID)
		                .period(30, TimeUnit.SECONDS)
		                .callable(new Callable<String>() {
		                    @Override
		                    public String call() throws Exception {
		                        return getManagedHostId();
		                    }
		                })
		                .onError(Functions.constant((String)null))
		                )
                .build();
    }
    
    public String getManagedHostId() {
        ClouderaManagerNode manager = getConfig(MANAGER);
        List<String> managedHosts = (manager == null) ? null : manager.getAttribute(ClouderaManagerNode.MANAGED_HOSTS);
        if (managedHosts == null || managedHosts.isEmpty()) return null;
        String hostname = getAttribute(HOSTNAME);
        if (groovyTruth(hostname) && managedHosts.contains(hostname)) return hostname;
        final String privateHostname = getAttribute(PRIVATE_HOSTNAME);
        if (groovyTruth(privateHostname)) {
            // manager might view it as ip-10-1-1-1.ec2.internal whereas node knows itself as just ip-10-1-1-1
            // TODO better might be to compare private IP addresses of this node with IP of managed nodes at CM  
            String pm = Iterables.find(managedHosts, new Predicate<String>() {
            	public boolean apply(String input) {
            		return input.startsWith(privateHostname);
            	}},
            	null);
            if (groovyTruth(pm)) return pm;
        }
        return null;
    }

    public ScriptHelper newScript(String summary) {
        return new ScriptHelper((ScriptRunner)getDriver(), summary);
    }

    /**
     * Start the entity in the given collection of locations.
     */
    @Description("Collect metrics files from this host and save to a file on this machine, as a subdir of the given dir, returning the name of that subdir")
    public String collectMetrics(@NamedParameter("targetDir") String targetDir) {
        targetDir = targetDir + "/" + getAttribute(CDH_HOST_ID);
        new File(targetDir).mkdir();
        // TODO allow wildcards, or batch on server then copy down?
        int i=0;
        for (String role : ImmutableList.of("datanode","namenode","master","regionserver")) {
            try {
                ((ClouderaCdhNodeSshDriver)getDriver()).getMachine().copyFrom(MutableMap.of("sshTries", 1),
                    "/tmp/"+role+"-metrics.out", targetDir+"/"+role+"-metrics.out");
            } catch (Exception e) {
                //not serious, file probably doesn't exist
                log.debug("Unable to copy /tmp/"+role+"-metrics.out from "+this+" (file may not exist): "+e);
            }
        }
        for (String role : ImmutableList.of("mr","jvm")) {
            try {
                ((ClouderaCdhNodeSshDriver)getDriver()).getMachine().copyFrom(MutableMap.of("sshTries", 1),
                    "/tmp/"+role+"metrics.log", targetDir+"/"+role+"metrics.log");
            } catch (Exception e) {
                //not serious, file probably doesn't exist
                log.debug("Unable to copy /tmp/"+role+"metrics.log from "+this+" (file may not exist): "+e);
            }
        }
        log.debug("Copied {} metrics files from {}", i, this);
        return targetDir;
    }
    
}
