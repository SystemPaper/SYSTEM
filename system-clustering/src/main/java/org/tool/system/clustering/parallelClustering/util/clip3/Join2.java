package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class Join2 implements JoinFunction<Tuple3<Edge, Tuple2<String, String>, String>, Tuple2<String, String>,
        Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>>> {
    @Override
    public Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>>
    join(Tuple3<Edge, Tuple2<String, String>, String> first, Tuple2<String, String> second) throws Exception {
        return Tuple3.of(first.f0, first.f1, second);
    }
}

