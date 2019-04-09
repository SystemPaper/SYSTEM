package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class GetObjNeighbors implements GroupReduceFunction <Tuple2<String, String>, Tuple1<String>> {
    @Override
    public void reduce(Iterable<Tuple2<String, String>> iterable, Collector<Tuple1<String>> collector) throws Exception {
        List<Tuple1<String>> neighbors = new ArrayList<>();
        Boolean hasOut = false;
        for (Tuple2<String, String> i:iterable){
            if(i.f1.equals("*"))
                hasOut = true;
            else
                neighbors.add(Tuple1.of(i.f1));
        }
        if (hasOut)
            for (Tuple1<String> i:neighbors)
                collector.collect(i);
    }
}
