package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */

//edge_srcId_trgtId.join(vertices).where(1).equalTo(0)
public class Join1 implements JoinFunction<Tuple3<Edge, String, String>, Tuple2<String, String>, Tuple3<Edge, Tuple2<String, String>, String>> {
    @Override
    public Tuple3<Edge, Tuple2<String, String>, String>
    join(Tuple3<Edge, String, String> first, Tuple2<String, String> second) throws Exception {
        return Tuple3.of(first.f0, second, first.f2);
    }
}
