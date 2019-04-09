package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Arrays;
import java.util.List;

public class GetSrcConsistents implements FlatMapFunction <Tuple4<Vertex, Vertex, String, String>, Tuple2<Vertex, Vertex>> {
    @Override
    public void flatMap(Tuple4<Vertex, Vertex, String, String> in, Collector<Tuple2<Vertex, Vertex>> collector) throws Exception {
        if (isConsistent (in.f2, in.f3))
            collector.collect(Tuple2.of(in.f0, in.f1));
    }
    private boolean isConsistent(String srces1, String srces2){
        String[] srcList1 = srces1.split(",");
        List<String> srcList2 = Arrays.asList(srces2.split(","));
        for(String s: srcList1){
            if (srcList2.contains(s))
                return false;
        }
        return true;
    }
}
