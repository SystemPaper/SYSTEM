package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AdvancedGetMaxBothVNewV implements GroupReduceFunction<Tuple3<Edge, Vertex, String>, Tuple3<Vertex, Vertex, Double>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, Vertex, String>> iterable, Collector<Tuple3<Vertex, Vertex, Double>> collector) throws Exception {
        Vertex v1 = null;
        Vertex v2 = null;
        Double sim = 0d;
//        Edge e = null;
        for (Tuple3<Edge, Vertex, String> it:iterable){
            if (v1==null)
                v1=it.f1;
            else
                v2=it.f1;
            sim = Double.parseDouble(it.f0.getPropertyValue("value").toString());
//            e = it.f0;
        }
//        if ((v1!=null &&v1.getPropertyValue("recId").toString().equals("288"))||
//                (v2!=null && v2.getPropertyValue("recId").toString().equals("288"))){
//            System.out.println(e.getId()+"---"+v1.getPropertyValue("recId")+"---"+v2.getPropertyValue("recId"));
//        }
//
//        if(v2==null)
//            System.out.println("v2.null: "+","+e.getId());
        if (v2 != null) {
//            boolean c1 = v1.hasProperty("new");
//            boolean c2 = v2.hasProperty("new");
//            if (c1!=c2 && (c1 || c2))
            if(v1.hasProperty("new"))
                collector.collect(Tuple3.of(v2, v1, sim));
            else
                collector.collect(Tuple3.of(v1, v2, sim));

        }

    }
}
