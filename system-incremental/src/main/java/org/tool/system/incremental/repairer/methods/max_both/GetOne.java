package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetOne implements GroupReduceFunction <Tuple3<Vertex, Vertex, String>, Tuple2<Vertex, Vertex>> {
    private int grpByPos;
    public GetOne (int grpByPos){
        this.grpByPos = grpByPos;
    }
    @Override
    public void reduce(Iterable<Tuple3<Vertex, Vertex, String>> iterable, Collector<Tuple2<Vertex, Vertex>> collector) throws Exception {
        Tuple2<Vertex, Vertex> selected = null;
        for (Tuple3<Vertex, Vertex, String> it:iterable){
            if (selected == null)
                selected = Tuple2.of(it.f0, it.f1);
            else if (grpByPos == 0) {
                if (selected.f1.getId().toString().compareTo(it.f1.getId().toString()) < 0)
                    selected = Tuple2.of(it.f0, it.f1);
            }
            else {
                if (selected.f0.getId().toString().compareTo(it.f0.getId().toString()) < 0)
                    selected = Tuple2.of(it.f0, it.f1);
            }
        }
        collector.collect(selected);

    }
}
