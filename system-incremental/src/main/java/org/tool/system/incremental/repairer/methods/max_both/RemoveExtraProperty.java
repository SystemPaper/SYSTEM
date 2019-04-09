package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

public class RemoveExtraProperty implements MapFunction <Vertex, Vertex> {
    @Override
    public Vertex map(Vertex vertex) throws Exception {
        if (vertex.hasProperty("new"))
            vertex.removeProperty("new");
        if (vertex.hasProperty("merged"))
            vertex.removeProperty("merged");
        return vertex;
    }
}
