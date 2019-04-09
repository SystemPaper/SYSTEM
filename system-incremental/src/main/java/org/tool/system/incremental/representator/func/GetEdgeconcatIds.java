package org.tool.system.incremental.representator.func;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

public class GetEdgeconcatIds implements MapFunction <Edge, Tuple2<Edge,String>> {
    @Override
    public Tuple2<Edge,String> map(Edge edge) throws Exception {
        if(edge.getSourceId().compareTo(edge.getTargetId()) < 0)
            return Tuple2.of(edge, edge.getSourceId()+"_"+edge.getTargetId());
        else
            return Tuple2.of(edge, edge.getTargetId()+"_"+edge.getSourceId());
    }
}
