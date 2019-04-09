package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetInterNewOld implements FlatMapFunction<Tuple3<Edge, Vertex, Vertex>, Tuple3<Edge, Vertex, Vertex>> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple3<Edge, Vertex, Vertex>> collector) throws Exception {
        if (in.f1.hasProperty("new") && !in.f2.hasProperty("new"))
            collector.collect(in);
        else if (!in.f1.hasProperty("new") && in.f2.hasProperty("new"))
            collector.collect(in);
    }
}

