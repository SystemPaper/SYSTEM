package org.tool.system.common.maxDeltaLinkSelection.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;


/**
 */
public class Link2Link_EndId_EndType implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Tuple3<Edge, String, String>>{

    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> value, Collector<Tuple3<Edge, String, String>> out) throws Exception {
        if (value.f1.hasProperty("type")) {
            out.collect(Tuple3.of(value.f0, value.f1.getId().toString(), value.f2.getPropertyValue("type").toString()));
            out.collect(Tuple3.of(value.f0, value.f2.getId().toString(), value.f1.getPropertyValue("type").toString()));
        }
        else {
            out.collect(Tuple3.of(value.f0, value.f1.getId().toString(), value.f2.getPropertyValue("graphLabel").toString()));
            out.collect(Tuple3.of(value.f0, value.f2.getId().toString(), value.f1.getPropertyValue("graphLabel").toString()));
        }
    }
}
