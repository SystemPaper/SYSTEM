package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AdvancedAddClsIds implements MapFunction <Tuple3<Vertex, Vertex, Double>,Tuple5<Vertex, Vertex, String, String, Double>> {
    @Override
    public Tuple5<Vertex, Vertex, String, String, Double> map(Tuple3<Vertex, Vertex, Double> in) throws Exception {
        return Tuple5.of(in.f0, in.f1, in.f0.getPropertyValue("ClusterId").toString(), in.f1.getPropertyValue("ClusterId").toString(), in.f2);
    }
}
