package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddClsId implements MapFunction <Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, String>> {
    private int goalVertex;
    public AddClsId (int goalVertex){
        this.goalVertex = goalVertex;
    }
    @Override
    public Tuple3<Vertex, Vertex, String> map(Tuple2<Vertex, Vertex> in) throws Exception {
        if (goalVertex == 0)
            return Tuple3.of(in.f0, in.f1, in.f0.getPropertyValue("ClusterId").toString());
        else
            return Tuple3.of(in.f0, in.f1, in.f1.getPropertyValue("ClusterId").toString());
    }
}
