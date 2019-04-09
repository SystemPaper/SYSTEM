package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetNonGPVs implements FlatMapFunction <Tuple3<Edge, Vertex, Boolean>, Vertex> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Boolean> in, Collector<Vertex> collector) throws Exception {
        if (!in.f2)
            collector.collect(in.f1);
    }











}
