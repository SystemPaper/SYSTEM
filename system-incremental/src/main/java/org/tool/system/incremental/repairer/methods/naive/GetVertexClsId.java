package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
//out: v_clsId_vId
public class GetVertexClsId implements FlatMapFunction<Tuple3<Edge,Vertex, Vertex>, Tuple3<Vertex, String, String>> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple3<Vertex, String, String>> collector) throws Exception {
//        if (!in.f1.hasProperty("ClusterId"))
//            System.out.println(in.f1.getId());
//        if (!in.f2.hasProperty("ClusterId"))
//            System.out.println(in.f2.getId());
        collector.collect(Tuple3.of(in.f1, in.f1.getPropertyValue("ClusterId").toString(), in.f1.getId().toString()));
        collector.collect(Tuple3.of(in.f2, in.f2.getPropertyValue("ClusterId").toString(), in.f2.getId().toString()));
    }
}
