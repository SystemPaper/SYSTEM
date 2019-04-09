package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */

public class Vertex2Vertex_GradoopId implements MapFunction<Vertex, Tuple2<Vertex, String>>
{
    @Override
    public Tuple2<Vertex, String> map(Vertex in) throws Exception {
        return Tuple2.of(in, in.getId().toString());
    }
}

