package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;

public class RemoveNewProperty implements MapFunction <Vertex, Vertex>{
    @Override
    public Vertex map(Vertex vertex) throws Exception {
        vertex.removeProperty("new");
        return vertex;
    }
}
