package org.tool.system.linking.linking.temp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class filterOutFarPoints implements FlatMapFunction<Tuple3<Vertex, Vertex, Double>, Tuple3<Vertex, Vertex, Double>> {
    @Override
    public void flatMap(Tuple3<Vertex, Vertex, Double> in, Collector<Tuple3<Vertex, Vertex, Double>> collector) throws Exception {
        if (in.f0.hasProperty("lon") && in.f0.hasProperty("lat") &&
                in.f1.hasProperty("lon") && in.f1.hasProperty("lat")) {

            double lat1 = Double.parseDouble(in.f0.getPropertyValue("lat").toString());
            double lon1 = Double.parseDouble(in.f0.getPropertyValue("lon").toString());

            double lat2 = Double.parseDouble(in.f1.getPropertyValue("lat").toString());
            double lon2 = Double.parseDouble(in.f1.getPropertyValue("lon").toString());

            Double arccosParam = Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon1 - lon2);
            if (arccosParam < -1)
                arccosParam = -1d;
            if (arccosParam > 1)
                arccosParam = 1d;
            double distance = Math.acos(arccosParam) * 6371;
            if (distance <= 1358)
                collector.collect(in);
        } else
            collector.collect(in);
    }
}
