package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class RemoveDoubles implements GroupReduceFunction<Tuple2<Vertex, String>, Vertex> {

    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> iterable, Collector<Vertex> collector) throws Exception {
        Vertex v = null;
        for(Tuple2<Vertex, String> it:iterable){
            if (v == null)
                v = it.f0;
            else if (it.f0.hasProperty("merged")) {
                v = it.f0;
            }
        }
        collector.collect(v);
    }
}
