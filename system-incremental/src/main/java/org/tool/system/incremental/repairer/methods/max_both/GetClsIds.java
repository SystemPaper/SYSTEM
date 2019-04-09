package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetClsIds implements MapFunction <Tuple2<Vertex, Vertex>, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> map(Tuple2<Vertex, Vertex> in) throws Exception {
        return Tuple2.of(in.f0.getPropertyValue("ClusterId").toString(), in.f1.getPropertyValue("ClusterId").toString());
    }
}
