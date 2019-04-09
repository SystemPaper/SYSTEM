package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;


/**
 *
 */

public class Minus2<A> {
    private DataSet<Tuple2<String, A>> first;
    private DataSet<Tuple2<String, A>> second;
    public Minus2(DataSet<Tuple2<String, A>> first, DataSet<Tuple2<String, A>> second){
        this.first = first;
        this.second = second;
    }

    public DataSet<A> execute (){
        DataSet<Tuple3<A, String, String>> firstSet = first.map(new identify("first"));
        DataSet<Tuple3<A, String, String>> secondSet = second.map(new identify("sec"));
        return firstSet.union(secondSet).groupBy(1).reduceGroup(new reducer());
    }

    private class identify<A> implements MapFunction<Tuple2<String,A >, Tuple3<A, String, String>> {
        private String id;
        public identify(String ID) {
            id = ID;
        }

        @Override
        public Tuple3<A, String, String> map(Tuple2<String, A> value) throws Exception {
            return Tuple3.of(value.f1, value.f0, id);
        }
    }

    private class reducer implements GroupReduceFunction<Tuple3<A, String, String>, A> {
        @Override
        public void reduce(Iterable<Tuple3<A, String, String>> values, Collector<A> out) throws Exception {
            int cnt = 0;
            A a = null;
            String type = "";
            for (Tuple3<A, String, String> v: values) {
                a = v.f0;
                type = v.f2;
                cnt++;
            }
            if (cnt == 1 && type.equals("first"))
                out.collect(a);
        }
    }
}