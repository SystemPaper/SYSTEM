package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetObjectClsIds implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Tuple1<String>> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple1<String>> collector) throws Exception {
        if(!in.f1.hasProperty("ClusterId") && in.f2.hasProperty("ClusterId"))
            collector.collect(Tuple1.of(in.f2.getPropertyValue("ClusterId").toString()));
        else if(in.f1.hasProperty("ClusterId") && !in.f2.hasProperty("ClusterId"))
            collector.collect(Tuple1.of(in.f1.getPropertyValue("ClusterId").toString()));
    }
}
