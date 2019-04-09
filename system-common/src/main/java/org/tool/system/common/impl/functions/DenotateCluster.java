package org.tool.system.common.impl.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.tool.system.common.impl.Cluster;

/**
 */
public class DenotateCluster implements MapFunction <Cluster, Cluster>{
    private Integer sourceNo;
    public DenotateCluster(Integer SourceNo){sourceNo = SourceNo;}
    public Cluster map(Cluster in)  {
        in.setIsCompletePerfect(sourceNo);
        return in;
    }
}
