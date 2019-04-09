package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AdvancedGetSrcJoin implements JoinFunction <Tuple5<Vertex, Vertex, String, String, Double>, Tuple2<String,String>, Tuple5<Vertex, Vertex, String, String, Double>> {
    private int joinPos;
    public AdvancedGetSrcJoin(int joinPos){
        this.joinPos = joinPos;
    }
    @Override
    public Tuple5<Vertex, Vertex, String, String, Double> join(Tuple5<Vertex, Vertex, String, String, Double> in1, Tuple2<String, String> in2) throws Exception {
//        String src = "";
//        if (joinPos == 2) {
//            if (in1.f0.hasProperty("graphLabel"))
//                src = in1.f0.getPropertyValue("graphLabel").toString();
//            else
//                src = in1.f0.getPropertyValue("type").toString();
//            return Tuple4.of(in1.f0, in1.f1, src, in1.f3);
//        }
//        else {
//            if (in1.f1.hasProperty("graphLabel"))
//                src = in1.f1.getPropertyValue("graphLabel").toString();
//            else
//                src = in1.f1.getPropertyValue("type").toString();
//            return Tuple4.of(in1.f0, in1.f1, in1.f2, src);
//
//        }
        String srces = in2.f1;
        if (joinPos == 2)
            return Tuple5.of(in1.f0, in1.f1, srces, in1.f3, in1.f4);
        else
            return Tuple5.of(in1.f0, in1.f1, in1.f2, srces, in1.f4);


    }
}
