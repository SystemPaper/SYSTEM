package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

public class ResetVertexGraphId implements MapFunction<Vertex, Vertex> {
    @Override
    public Vertex map(Vertex vertex) throws Exception {
        vertex.resetGraphIds();
//        vertex.removeProperty("ClusterId");
        return vertex;
    }
}
