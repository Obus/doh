package doh2.impl;


import doh.api.TempPathManager;
import doh.op.JobRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DSExecutor {

    private final OpExecutor opExecutor;

    public DSExecutor(TempPathManager tempPathManager, Configuration conf, Class<? extends OutputFormat> defaultOutputFormatClass, JobRunner jobRunner, OpJobMaker opJobMaker) {
        this.opExecutor = new OpExecutor(tempPathManager, conf, defaultOutputFormatClass, jobRunner, opJobMaker);
    }

    public void execute(OnDemandDS... dses) throws Exception {
        Set<OnDemandDS> origins = new HashSet<OnDemandDS>();
        for (OnDemandDS ds : dses) {
            origins.add(findOrigin(ds));
        }
        List<OnDemandDS.ExecutionNode> rootNodes = new ArrayList<OnDemandDS.ExecutionNode>(origins.size());
        for (OnDemandDS o : origins) {
            rootNodes.add(o.node());
        }

        ExecutionGraph executionGraph = new ExecutionGraph(rootNodes, opExecutor);
        for (ExecutionUnit executionUnit : executionGraph.executionUnits()) {
            opExecutor.execute(executionUnit);
        }
    }


    public OnDemandDS findOrigin(OnDemandDS ds) {
        OnDemandDS ancestor = ds;
        while (!ancestor.isReady()) {
            ancestor = ancestor.parent();
        }
        return ancestor;
    }
}
