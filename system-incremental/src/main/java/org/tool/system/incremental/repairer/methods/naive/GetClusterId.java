package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
// in: maxBothE_V_V
// out: maxBoth_E_V_clsId
public class GetClusterId implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Tuple3<Edge, Vertex, String>> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple3<Edge, Vertex, String>> collector) throws Exception {
        collector.collect(Tuple3.of(in.f0, in.f1, in.f1.getPropertyValue("ClusterId").toString()));
        collector.collect(Tuple3.of(in.f0, in.f2, in.f2.getPropertyValue("ClusterId").toString()));
    }
}
