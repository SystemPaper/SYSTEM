package org.tool.system.example.EntityConfigMaker.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;


public class SetIncId implements MapFunction<Tuple2<Long, Vertex>, Vertex> {
    private int inc;
    private int plus;
    public SetIncId(int inc, int plus) {
        this.inc = inc;
        this.plus = plus;
    }

    @Override
    public Vertex map(Tuple2<Long, Vertex> id_vertex) throws Exception {
        id_vertex.f1.setProperty("inc",(id_vertex.f0%inc)+plus);
        return id_vertex.f1;
    }
}