package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetOldNewClsIds implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Tuple2<String, String>> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple2<String, String>> collector) throws Exception {
        String oldClsId = "";
        String replacingClsId = "";
        if (in.f1.hasProperty("new")) {
            oldClsId = in.f1.getPropertyValue("ClusterId").toString();
            replacingClsId = in.f2.getPropertyValue("ClusterId").toString();
        }
        else {
            oldClsId = in.f2.getPropertyValue("ClusterId").toString();
            replacingClsId = in.f1.getPropertyValue("ClusterId").toString();
        }
       collector.collect(Tuple2.of(oldClsId, replacingClsId));
    }
}
