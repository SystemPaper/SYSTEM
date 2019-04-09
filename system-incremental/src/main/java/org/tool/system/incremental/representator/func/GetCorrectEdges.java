package org.tool.system.incremental.representator.func;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

public class GetCorrectEdges implements FlatMapFunction<Tuple2<Edge,Boolean>, Edge> {

    @Override
    public void flatMap(Tuple2<Edge, Boolean> in, Collector<Edge> collector) throws Exception {
        if (in.f1)
            collector.collect(in.f0);
    }
}
