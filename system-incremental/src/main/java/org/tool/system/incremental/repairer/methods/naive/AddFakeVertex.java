package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddFakeVertex implements MapFunction <Tuple2<String, String>, Tuple3<Vertex, String, String>> {
    @Override
    public Tuple3<Vertex, String, String> map(Tuple2<String, String> in) throws Exception {
        return Tuple3.of(new Vertex(), in.f0, in.f1);
    }
}
