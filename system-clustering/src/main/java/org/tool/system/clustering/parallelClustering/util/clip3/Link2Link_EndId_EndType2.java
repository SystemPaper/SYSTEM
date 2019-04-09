package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;


/**
 */
public class Link2Link_EndId_EndType2 implements FlatMapFunction <Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>>,
        Tuple3<Edge, String, String>>{

    @Override
    public void flatMap(Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>> value, Collector<Tuple3<Edge, String, String>> out) throws Exception {

            out.collect(Tuple3.of(value.f0, value.f1.f0, value.f1.f1));
        out.collect(Tuple3.of(value.f0, value.f2.f0, value.f2.f1));

    }
}
