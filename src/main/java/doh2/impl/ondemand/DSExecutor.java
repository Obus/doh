package doh2.impl.ondemand;


import doh2.api.DSContext;

import java.util.ArrayList;
import java.util.List;

public class DSExecutor {

    private final JobPackExecutor jobPackExecutor;
    private final JobPacker jobPacker;

    public DSExecutor(DSContext dsContext) {
        this.jobPackExecutor = new JobPackExecutor(dsContext, new OpJobMaker());
        this.jobPacker = new JobPacker();
    }

    public void execute(OnDemandDS... dses) throws Exception {
        List<OnDemandDS.DSExecutionNode> targetNodes = new ArrayList<OnDemandDS.DSExecutionNode>();
        for (OnDemandDS ds : dses) {
            targetNodes.add(ds.node());
        }

        ExecutionGraph<OnDemandDS.DSExecutionNode> executionGraph = new ExecutionGraph<OnDemandDS.DSExecutionNode>(targetNodes);
        for (ExecutionUnit<OnDemandDS.DSExecutionNode> executionUnit : executionGraph.executionUnits()) {
            for (JobPack jobPack : jobPacker.packJobs(executionUnit)) {
                jobPackExecutor.execute(jobPack);
            }
        }
    }


}
