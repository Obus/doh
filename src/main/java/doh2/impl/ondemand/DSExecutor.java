package doh2.impl.ondemand;


import doh2.api.DSContext;

import java.util.ArrayList;
import java.util.List;

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
