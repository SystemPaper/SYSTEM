package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetMaxBoth implements GroupReduceFunction <Tuple3<Edge, Vertex, String>, Tuple3<Edge, Vertex, Vertex>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, Vertex, String>> iterable, Collector<Tuple3<Edge, Vertex, Vertex>> collector) throws Exception {
        Vertex v1 = null;
        Vertex v2 = null;
        Edge e = null;
        int i = 0;
        for (Tuple3<Edge, Vertex, String> it:iterable){
            e = it.f0;
            if (i==0)
                v1=it.f1;
            else
                v2=it.f1;
            i++;

        }
//        if ((v1!=null &&v1.getPropertyValue("recId").toString().equals("288"))||
//                (v2!=null && v2.getPropertyValue("recId").toString().equals("288"))){
//            System.out.println(e.getId()+"---"+v1.getPropertyValue("recId")+"---"+v2.getPropertyValue("recId"));
//        }
//

        if (v2 != null) {
            boolean c1 = v1.hasProperty("new");
            boolean c2 = v2.hasProperty("new");
            if (c1!=c2 && (c1 || c2))
                collector.collect(Tuple3.of(e, v1, v2));
        }
    }
}
