package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class Link2Link_Id implements MapFunction <Edge, Tuple2<Edge, String>>{
    @Override
    public Tuple2<Edge, String> map(Edge value) throws Exception {
        return Tuple2.of(value, value.getId().toString());
    }
}
