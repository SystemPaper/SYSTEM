package org.tool.system.example.EntityConfigMaker.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetSrc implements FlatMapFunction<Vertex, Vertex> {
    String src;
    public GetSrc(String src) {
        this.src = src;
    }

    @Override
    public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
        if (vertex.getPropertyValue("graphLabel").toString().equals(src))
            collector.collect(vertex);
    }
}
