package org.tool.system.incremental.representator.func;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

public class GetAverage implements GroupReduceFunction <Tuple3<Edge, String, String>, Tuple3<Edge, String, String>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, String, String>> iterable, Collector<Tuple3<Edge, String, String>> collector) throws Exception {
        int cnt = 0;
        Double aveSim = 0d;
        Edge edge = null;

        for (Tuple3<Edge, String, String> item:iterable){
            cnt++;
            edge = item.f0;
            aveSim+= Double.parseDouble(edge.getPropertyValue("value").toString());
        }
        if (cnt == 1)
            collector.collect(Tuple3.of(edge, edge.getSourceId().toString(), edge.getTargetId().toString()));
        else {
            aveSim /= cnt;
            edge.setProperty("value", aveSim);
            collector.collect(Tuple3.of(edge, edge.getSourceId().toString(), edge.getTargetId().toString()));
        }

    }
}
