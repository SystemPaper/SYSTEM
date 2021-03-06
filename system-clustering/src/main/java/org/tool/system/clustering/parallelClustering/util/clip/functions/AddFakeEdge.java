package org.tool.system.clustering.parallelClustering.util.clip.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class AddFakeEdge implements MapFunction <Tuple2<Vertex, String>, Tuple2<Edge, String>>{
    @Override
    public Tuple2<Edge, String> map(Tuple2<Vertex, String> vertex_vertexId) throws Exception {
        return Tuple2.of(new Edge(),vertex_vertexId.f1);
    }
}
