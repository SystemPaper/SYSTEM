package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetMaxBothVNewVSrcs implements GroupReduceFunction<Tuple3<String, Vertex, String>, Tuple4<Vertex, Vertex, String, String>> {
    @Override
    public void reduce(Iterable<Tuple3<String, Vertex, String>> iterable, Collector<Tuple4<Vertex, Vertex, String, String>> collector) throws Exception {
        Vertex v1 = null;
        Vertex v2 = null;
        String srcs1 = "";
        String srcs2 = "";

        for (Tuple3<String, Vertex, String> it:iterable){
            if (v1==null) {
                v1 = it.f1;
                srcs1 = it.f2;
            }
            else {
                v2 = it.f1;
                srcs2 = it.f2;
            }
        }

        if (v2 != null) {

            if(v1.hasProperty("new"))
                collector.collect(Tuple4.of(v2, v1, srcs2, srcs1));
            else
                collector.collect(Tuple4.of(v1, v2, srcs1, srcs2));

        }
    }
}
