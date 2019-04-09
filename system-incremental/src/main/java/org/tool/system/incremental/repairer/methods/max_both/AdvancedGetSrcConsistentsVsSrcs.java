package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Arrays;
import java.util.List;

public class AdvancedGetSrcConsistentsVsSrcs implements FlatMapFunction<Tuple5<Vertex, Vertex, String, String, Double>, Tuple5<Vertex, Vertex, String, String, Double>> {
    @Override
    public void flatMap(Tuple5<Vertex, Vertex, String, String, Double> in, Collector<Tuple5<Vertex, Vertex, String, String, Double>> collector) throws Exception {
        if (isConsistent(in.f2, in.f3))
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
