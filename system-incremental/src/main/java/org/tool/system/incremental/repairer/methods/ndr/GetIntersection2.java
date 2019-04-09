package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.List;

public class GetIntersection2 implements GroupReduceFunction <Tuple3<Edge,Vertex, String>, Tuple3<Edge,Vertex, Boolean>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, Vertex, String>> iterable, Collector<Tuple3<Edge, Vertex, Boolean>> collector) throws Exception {
        List<Tuple2<Edge, Vertex>> link_vertex = new ArrayList<>();
        Boolean hasOut = false;
        for (Tuple3<Edge, Vertex, String> i:iterable){
            if(i.f1.getId() == null)
                hasOut = true;
            else
                link_vertex.add(Tuple2.of(i.f0, i.f1));
        }
//        if (hasOut)

            for (Tuple2<Edge, Vertex> i:link_vertex)
                collector.collect(Tuple3.of(i.f0, i.f1, hasOut));
    }
}
