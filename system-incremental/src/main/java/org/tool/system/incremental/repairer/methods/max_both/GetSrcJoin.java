package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetSrcJoin implements JoinFunction <Tuple4<Vertex, Vertex, String, String>, Tuple2<String,String>, Tuple4<Vertex, Vertex, String, String>> {
    private int joinPos;
    public GetSrcJoin (int joinPos){
        this.joinPos = joinPos;
    }
    @Override
    public Tuple4<Vertex, Vertex, String, String> join(Tuple4<Vertex, Vertex, String, String> in1, Tuple2<String, String> in2) throws Exception {
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
            return Tuple4.of(in1.f0, in1.f1, srces, in1.f3);
        else
            return Tuple4.of(in1.f0, in1.f1, in1.f2, srces);


    }
}
