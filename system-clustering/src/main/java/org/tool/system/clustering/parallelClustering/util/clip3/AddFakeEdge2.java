package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class AddFakeEdge2 implements MapFunction <Tuple3<String, String, String>, Tuple2<Edge, String>>{
    @Override
    public Tuple2<Edge, String> map(Tuple3<String, String, String> vertex) throws Exception {
        return Tuple2.of(new Edge(),vertex.f0);
    }
}
