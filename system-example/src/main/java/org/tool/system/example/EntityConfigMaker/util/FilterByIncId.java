package org.tool.system.example.EntityConfigMaker.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;


public class FilterByIncId implements FlatMapFunction<Vertex, Vertex> {
    int incId;
    public FilterByIncId(int i) {
        incId = i;
    }

    @Override
    public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
        if (Integer.parseInt(vertex.getPropertyValue("inc").toString()) == incId)
            collector.collect(vertex);
    }
}