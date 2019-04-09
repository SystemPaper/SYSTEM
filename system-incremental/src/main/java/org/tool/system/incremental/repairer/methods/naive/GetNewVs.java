package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetNewVs implements FlatMapFunction <Tuple2<Vertex, Boolean>, Vertex>{
    @Override
    public void flatMap(Tuple2<Vertex, Boolean> in, Collector<Vertex> collector) throws Exception {
        if (in.f1)
            collector.collect(in.f0);
    }
}
