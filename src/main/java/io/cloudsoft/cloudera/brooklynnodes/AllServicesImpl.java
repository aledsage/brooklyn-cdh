package io.cloudsoft.cloudera.brooklynnodes;

import java.io.File;
import java.util.List;
import java.util.Map;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Description;

import com.google.common.collect.Lists;

public class AllServicesImpl extends StartupGroupImpl implements AllServices {

	public AllServicesImpl() {
	}

	public AllServicesImpl(Map flags) {
		super(flags);
	}

	public AllServicesImpl(Entity parent) {
		super(parent);
	}

	public AllServicesImpl(Map flags, Entity parent) {
		super(flags, parent);
	}
	
    /**
     * Start the entity in the given collection of locations.
     */
    @Description("Collect metrics files from all hosts and save to a file on this machine, returning the name of that subdir")
    public String collectMetrics() {
        String name = "cloudera-metrics-"+System.currentTimeMillis();
        String targetBaseDir = "/tmp/cloudera-metrics/";
        new File(targetBaseDir).mkdir();
        String targetDir = targetBaseDir+"/"+name;
        new File(targetDir).mkdir();
        List<ClouderaCdhNode> nodes = Lists.newArrayList();
        collectNodes(getApplication(), nodes);
        for (ClouderaCdhNode node : nodes) {
        	node.collectMetrics(targetDir);
        }
        return targetDir;
    }
    
    protected void collectNodes(Entity root, List<ClouderaCdhNode> list) {
        if (root instanceof ClouderaCdhNode) {
        	list.add((ClouderaCdhNode)root);
        } else {
        	for (Entity child: root.getChildren()) {
        		collectNodes(child, list);
        	}
        }
    }
}
