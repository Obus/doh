package doh2.impl;


import doh.api.TempPathManager;
import doh.op.JobRunner;
import doh2.api.DSContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DSExecutor {

    private final OpExecutor opExecutor;

    public DSExecutor(DSContext dsContext) {
        this.opExecutor = new OpExecutor(dsContext);
    }

    public void execute(OnDemandDS... dses) throws Exception {
        List<OnDemandDS.ExecutionNode> targetNodes = new ArrayList<OnDemandDS.ExecutionNode>();
        for (OnDemandDS ds : dses) {
            targetNodes.add(ds.node());
        }

        ExecutionGraph executionGraph = new ExecutionGraph(targetNodes, opExecutor);
        for (ExecutionUnit executionUnit : executionGraph.executionUnits()) {
            opExecutor.execute(executionUnit);
        }
    }


}
