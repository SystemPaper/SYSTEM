package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AdvancedAddClsId1 implements MapFunction<Tuple5<Vertex, Vertex, String, String, Double>, Tuple6<Vertex, Vertex, String, String, String, Double>> {
    private int goalVertex;
    public AdvancedAddClsId1(int goalVertex){
        this.goalVertex = goalVertex;
    }
    @Override
    public Tuple6<Vertex, Vertex, String, String, String, Double> map(Tuple5<Vertex, Vertex, String, String, Double> in) throws Exception {
        if (goalVertex == 0)
            return Tuple6.of(in.f0, in.f1,in.f2, in.f3,in.f0.getPropertyValue("ClusterId").toString(), in.f4);
        else
            return Tuple6.of(in.f0, in.f1, in.f2, in.f3, in.f1.getPropertyValue("ClusterId").toString(), in.f4);
    }
}