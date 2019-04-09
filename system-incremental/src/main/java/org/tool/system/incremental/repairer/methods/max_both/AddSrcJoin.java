package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddSrcJoin implements JoinFunction<Tuple6<String, Double, Vertex, Vertex, String, String>, Tuple2<String,String>,
        Tuple6<String, Double, Vertex, Vertex, String, String>> {
    private int joinPos;
    public AddSrcJoin (int joinPos){
        this.joinPos = joinPos;
    }
    @Override
    public Tuple6<String, Double, Vertex, Vertex, String, String> join(Tuple6<String, Double, Vertex, Vertex, String, String> in1,
                                                                       Tuple2<String, String> in2) throws Exception {

        String srces = in2.f1;
        if (joinPos == 2)
            return Tuple6.of(in1.f0, in1.f1, in1.f2, in1.f3, srces, in1.f5);
        else
            return Tuple6.of(in1.f0, in1.f1, in1.f2, in1.f3, in1.f4, srces);
    }
}
