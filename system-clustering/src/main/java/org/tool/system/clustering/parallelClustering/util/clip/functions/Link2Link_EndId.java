package org.tool.system.clustering.parallelClustering.util.clip.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class Link2Link_EndId implements MapFunction <Edge, Tuple2<Edge, String>>{
    private Integer endType;
    public Link2Link_EndId(Integer inputEndType){endType = inputEndType;}
    @Override
    public Tuple2<Edge, String> map(Edge edge) throws Exception {
        if (endType == 0)
            return Tuple2.of(edge, edge.getSourceId().toString());
        else
            return Tuple2.of(edge, edge.getTargetId().toString());
    }
}
