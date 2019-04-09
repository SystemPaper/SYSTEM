package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
//in: v_clsId_vId
//out: fakeL_v_clsId
public class AddFakeLink implements MapFunction <Tuple2<Vertex, String>, Tuple3<Edge,Vertex, String>> {
    @Override
    public Tuple3<Edge, Vertex, String> map(Tuple2<Vertex, String> in) throws Exception {
        return Tuple3.of(new Edge(), in.f0, in.f1);
    }
}
