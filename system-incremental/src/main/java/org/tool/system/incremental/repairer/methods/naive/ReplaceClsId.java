package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.List;

public class ReplaceClsId implements GroupReduceFunction <Tuple3<Vertex, String, String>, Vertex> {
    @Override
    public void reduce(Iterable<Tuple3<Vertex, String, String>> iterable, Collector<Vertex> collector) throws Exception {
        String replacingClsId = "";
        List<Vertex> vertices = new ArrayList<>();
        List<String> vertexIds = new ArrayList<>();

        for (Tuple3<Vertex, String, String> it:iterable){
            if (!it.f2.equals(""))
                replacingClsId = it.f2;
            if (it.f0.getId()!=null && !vertexIds.contains(it.f0.getId().toString())){
                vertexIds.add(it.f0.getId().toString());
                vertices.add(it.f0);
            }
        }
        if (!replacingClsId.equals(""))
            for (Vertex vertex:vertices) {
                vertex.setProperty("ClusterId", replacingClsId);
                collector.collect(vertex);
            }
    }
}
