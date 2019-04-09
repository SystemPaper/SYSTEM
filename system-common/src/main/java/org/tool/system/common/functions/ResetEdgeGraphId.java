package org.tool.system.common.functions;


import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Edge;

public class ResetEdgeGraphId implements MapFunction<Edge, Edge> {
    @Override
    public Edge map(Edge edge) throws Exception {
        edge.resetGraphIds();
        return edge;
    }
}
