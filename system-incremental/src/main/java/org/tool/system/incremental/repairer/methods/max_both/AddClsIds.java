package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddClsIds implements MapFunction <Tuple2<Vertex, Vertex>,Tuple4<Vertex, Vertex, String, String>> {
    @Override
    public Tuple4<Vertex, Vertex, String, String> map(Tuple2<Vertex, Vertex> in) throws Exception {
        return Tuple4.of(in.f0, in.f1, in.f0.getPropertyValue("ClusterId").toString(), in.f1.getPropertyValue("ClusterId").toString());
    }
}
