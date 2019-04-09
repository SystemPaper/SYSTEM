package org.tool.system.incremental.repairer.methods.naive;


import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class MinusLST {
    private DataSet<Tuple4<Edge, Vertex, Vertex, String>> first;
    private DataSet<Tuple4<Edge, Vertex, Vertex, String>> sec;
    public MinusLST (DataSet<Tuple3<Edge, Vertex, Vertex>> first, DataSet<Tuple3<Edge, Vertex, Vertex>> sec){
        this.first = first.map(new AddId());
        this.sec = sec.map(new AddId());
    }

    public DataSet<Tuple3<Edge, Vertex, Vertex>> execute(){
        return first.union(sec).groupBy(3).reduceGroup(new MinusLSTReducer());
    }

    private class AddId implements MapFunction<Tuple3<Edge, Vertex, Vertex>, Tuple4<Edge, Vertex, Vertex, String>> {
        @Override
        public Tuple4<Edge, Vertex, Vertex, String> map(Tuple3<Edge, Vertex, Vertex> in) throws Exception {
            return Tuple4.of(in.f0, in.f1, in.f2, in.f0.getId().toString());
        }
    }

    private class MinusLSTReducer implements GroupReduceFunction<Tuple4<Edge, Vertex, Vertex, String>, Tuple3<Edge, Vertex, Vertex>> {
        @Override
        public void reduce(Iterable<Tuple4<Edge, Vertex, Vertex, String>> iterable, Collector<Tuple3<Edge, Vertex, Vertex>> collector) throws Exception {
            int cnt=0;
            Tuple3<Edge, Vertex, Vertex> output = null;
            for (Tuple4<Edge, Vertex, Vertex, String> it:iterable){
                cnt++;
                output = Tuple3.of(it.f0, it.f1, it.f2);
            }
            if (cnt==1)
                collector.collect(output);
        }
    }
}
