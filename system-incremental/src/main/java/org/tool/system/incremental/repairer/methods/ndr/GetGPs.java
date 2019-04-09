package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetGPs implements FlatMapFunction <Tuple3<Edge, Vertex, Boolean>, Tuple2<Edge, Vertex>> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Boolean> in, Collector<Tuple2<Edge, Vertex>> collector) throws Exception {
        if (in.f2)
            collector.collect(Tuple2.of(in.f0, in.f1));
    }
}
