package org.tool.system.incremental.representator.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;

public class OrderEndIds implements MapFunction <Tuple3<Edge, String, String>, Tuple3<Edge, String, String>> {
    @Override
    public Tuple3<Edge, String, String> map(Tuple3<Edge, String, String> input) throws Exception {
        if (input.f1.compareTo(input.f2) < 0 )
            return input;
        else
            return Tuple3.of(input.f0, input.f2, input.f1);
    }
}
