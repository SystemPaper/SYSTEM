package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetclusterNeighbors implements FlatMapFunction <Tuple2<Vertex, Vertex>, Tuple2<String, String>> {
    @Override
    public void flatMap(Tuple2<Vertex, Vertex> in, Collector<Tuple2<String, String>> collector) throws Exception {
        String clsId1 = "";
        String clsId2 = "";

        if(in.f0.hasProperty("ClusterId"))
            clsId1 = in.f0.getPropertyValue("ClusterId").toString();
        if(in.f1.hasProperty("ClusterId"))
            clsId2 = in.f1.getPropertyValue("ClusterId").toString();

        if (!(clsId1.equals(clsId2))){
            if (clsId1.equals(""))
                collector.collect(Tuple2.of(clsId1, clsId2));
            else if (clsId2.equals(""))
                collector.collect(Tuple2.of(clsId2, clsId1));
            else{
                collector.collect(Tuple2.of(clsId2, clsId1));
                collector.collect(Tuple2.of(clsId1, clsId2));
            }

        }
    }
}
