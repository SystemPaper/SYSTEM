package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

public class JoinMergeCluster implements JoinFunction <Tuple2<Vertex, String>, Tuple2<String, String>, Vertex> {
    @Override
    public Vertex join(Tuple2<Vertex, String> in1, Tuple2<String, String> in2) throws Exception {
        in1.f0.setProperty("ClusterId", in2.f0);
        in1.f0.setProperty("merged","");
        return in1.f0;
    }
}
