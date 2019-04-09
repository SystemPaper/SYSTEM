package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class MinusClsNeibors {
    private DataSet<Tuple2<String, String>> first;
    private DataSet<Tuple2<String, String>> sec;
    public MinusClsNeibors (DataSet<Tuple2<String, String>> cls1_cls2, DataSet<Tuple1<String>> clsId1){
        first = cls1_cls2;
        sec = clsId1.map(new MakeCurTuple2());
    }
    public DataSet<Tuple2<String, String>> execute(){
        return first.union(sec).groupBy(0).reduceGroup(new ThisMinus());
    }

    private class ThisMinus implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>> {
        @Override
        public void reduce(Iterable<Tuple2<String, String>> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
            List<Tuple2<String, String>> neighbors = new ArrayList<>();
            Boolean hasOut = true;
            for (Tuple2<String, String> i:iterable){
                if(i.f1.equals("*")) {
                    hasOut = false;
                    break;
                }
                else
                    neighbors.add(i);
            }
            if (hasOut)
                for (Tuple2<String, String> i:neighbors)
                    collector.collect(i);
        }
    }
}
