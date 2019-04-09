package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetLink_ST_ClsId implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>,Tuple3<Edge,Vertex, String> > {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple3<Edge, Vertex, String>> collector) throws Exception {
        String clsId1="";
        String clsId2 = "";
        if (in.f1.hasProperty("ClusterId"))
            clsId1 = in.f1.getPropertyValue("ClusterId").toString();
        if (in.f2.hasProperty("ClusterId"))
            clsId2 = in.f2.getPropertyValue("ClusterId").toString();

        collector.collect(Tuple3.of(in.f0, in.f1,clsId1));
        collector.collect(Tuple3.of(in.f0, in.f2,clsId2));
    }
}
