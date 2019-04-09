package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class MakeAllTuple3 implements MapFunction <Tuple1<String>, Tuple3<Edge,Vertex, String>> {
    @Override
    public Tuple3<Edge, Vertex, String> map(Tuple1<String> in) throws Exception {
        return Tuple3.of(new Edge(), new Vertex(), in.f0);
    }
}
