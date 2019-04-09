package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.List;

public class LateGetSources implements GroupReduceFunction <Tuple3<Edge, Vertex, String>, Tuple3<Edge, Vertex, String>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, Vertex, String>> iterable, Collector<Tuple3<Edge, Vertex, String>> collector) throws Exception {
        List<Tuple2<Edge, Vertex>> edge_vertexList = new ArrayList<>();
        List<String> srces = new ArrayList();
        for (Tuple3<Edge, Vertex, String> it:iterable){
            if (it.f0.getId() != null)
                edge_vertexList.add(Tuple2.of(it.f0, it.f1));
            String src = it.f1.getPropertyValue("graphLabel").toString();
            if (!srces.contains(src))
                srces.add(src);
        }
        String srcesStr = "";
        for (String s: srces)
            srcesStr += ("," + s);
        srcesStr = srcesStr.substring(1);
        for (Tuple2<Edge, Vertex> l_v: edge_vertexList)
            collector.collect(Tuple3.of(l_v.f0, l_v.f1, srcesStr));
    }
}
