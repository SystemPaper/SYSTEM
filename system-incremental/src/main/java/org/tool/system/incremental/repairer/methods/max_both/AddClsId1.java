package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddClsId1 implements MapFunction<Tuple4<Vertex, Vertex, String, String>, Tuple5<Vertex, Vertex, String, String, String>> {
    private int goalVertex;
    public AddClsId1 (int goalVertex){
        this.goalVertex = goalVertex;
    }
    @Override
    public Tuple5<Vertex, Vertex, String, String, String> map(Tuple4<Vertex, Vertex, String, String> in) throws Exception {
        if (goalVertex == 0)
            return Tuple5.of(in.f0, in.f1,in.f2, in.f3,in.f0.getPropertyValue("ClusterId").toString());
        else
            return Tuple5.of(in.f0, in.f1, in.f2, in.f3, in.f1.getPropertyValue("ClusterId").toString());
    }
}