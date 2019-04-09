package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class GetClsIdSrces implements GroupReduceFunction <Tuple2<String,String>, Tuple2<String,String>> {
    @Override
    public void reduce(Iterable<Tuple2<String, String>> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
        String srces = "";
        String clsId = "";
        for (Tuple2<String, String> it:iterable){
            clsId = it.f0;
            srces+= (","+it.f1);
        }
        collector.collect(Tuple2.of(clsId, srces.substring(1)));
    }
}
