package org.tool.system.linking.linking.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class Vertex2Vertex_GraphGradoopId implements MapFunction<Vertex, Tuple2<Vertex, GradoopId>>{
    @Override
    public Tuple2<Vertex, GradoopId> map(Vertex vertex) throws Exception {
        return Tuple2.of(vertex, vertex.getGraphIds().iterator().next());
    }
}
