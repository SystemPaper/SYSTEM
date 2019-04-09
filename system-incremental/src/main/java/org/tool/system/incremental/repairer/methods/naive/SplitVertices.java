package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class SplitVertices implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Vertex> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Vertex> collector) throws Exception {
        collector.collect(in.f1);
        collector.collect(in.f2);
    }
}
