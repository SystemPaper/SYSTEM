package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
//in: maxBoth_E_V_srces
//out: maxBoth_E_V_srces_eId
public class AddEdgeId implements MapFunction <Tuple3<Edge, Vertex, String>, Tuple4<Edge, Vertex, String, String>> {
    @Override
    public Tuple4<Edge, Vertex, String, String> map(Tuple3<Edge, Vertex, String> in) throws Exception {
        return Tuple4.of(in.f0, in.f1, in.f2, in.f0.getId().toString());
    }
}
