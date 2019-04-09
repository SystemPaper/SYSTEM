package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Arrays;
import java.util.List;

public class GetSrcConsistentItems implements FlatMapFunction<Tuple6<String, Double, Vertex, Vertex, String, String>,
        Tuple6<String, Double, Vertex, Vertex, String, String>> {
    @Override
    public void flatMap(Tuple6<String, Double, Vertex, Vertex, String, String> in,
                        Collector<Tuple6<String, Double, Vertex, Vertex, String, String>> collector) throws Exception {
        if (isConsistent(in.f4, in.f5))
            collector.collect(in);
    }

    private boolean isConsistent(String srces1, String srces2) {
        String[] srcList1 = srces1.split(",");
        List<String> srcList2 = Arrays.asList(srces2.split(","));
        for (String s : srcList1) {
            if (srcList2.contains(s))
                return false;
        }
        return true;
    }
}