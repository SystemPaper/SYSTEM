package org.tool.system.linking.blocking.blocking_methods.cartesian_product.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 *
 */
public class FilterIntraPairs implements FlatMapFunction <Tuple2<Vertex, Vertex>, Tuple2<Vertex, Vertex>> {
    @Override
    public void flatMap(Tuple2<Vertex, Vertex> value, Collector<Tuple2<Vertex, Vertex>> out) throws Exception {
        if (!value.f0.getGraphIds().containsAny(value.f1.getGraphIds()))
            out.collect(value);
    }
}
