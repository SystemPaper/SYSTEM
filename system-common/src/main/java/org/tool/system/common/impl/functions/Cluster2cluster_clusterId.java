package org.tool.system.common.impl.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.tool.system.common.impl.Cluster;

/**
 *
 */
public class Cluster2cluster_clusterId implements MapFunction <Cluster, Tuple2<Cluster, String>>{
    @Override
    public Tuple2<Cluster, String> map(Cluster in) throws Exception {

        return Tuple2.of(in, in.getClusterId());
    }
}
