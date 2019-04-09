package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.List;

//in: intE_ST_id_src.groupBy(2,3)
//out: maxBothE_V_V
public class GetMax implements GroupReduceFunction <Tuple4<Edge, Vertex,String, String>, Tuple3<Edge, Vertex, String>> {
    @Override
    public void reduce(Iterable<Tuple4<Edge, Vertex, String, String>> iterable, Collector<Tuple3<Edge, Vertex, String>> collector) throws Exception {
        List<Tuple3<Edge, Vertex, String>> l_v_lId = new ArrayList<>();
        Double maxSim = -1d;
        for (Tuple4<Edge, Vertex, String, String> it:iterable){

            Double sim = Double.parseDouble(it.f0.getPropertyValue("value").toString());
//            if (sim > maxSim){
            if (sim.compareTo(maxSim) > 0){
                maxSim = sim;
                l_v_lId.clear();
                l_v_lId.add(Tuple3.of(it.f0, it.f1, it.f0.getId().toString()));
            }
            else if (sim.equals(maxSim)) {
                l_v_lId.add(Tuple3.of(it.f0, it.f1, it.f0.getId().toString()));
            }
//            if(it.f1.getPropertyValue("recId").toString().equals("288"))
//            {
//                t=true;
//                s=it.f3;
////                System.out.println(sim.equals(maxSim));
////                Double sim1 = Double.parseDouble(it.f0.getPropertyValue("value").toString());
////                System.out.println(sim1);
//                System.out.println(it.f0.getId()+","+it.f0.getPropertyValue("value")+"---"+it.f2+"---"+it.f3);
//            }
        }
//        if (!t.equals("")) {
//            for (Tuple3<Edge, Vertex, String> item : l_v_lId) {
//                if (item.f0.getSourceId().toString().equals(t) || item.f0.getTargetId().toString().equals(t))
//                    System.out.println(item.f0.getId() + "---" + item.f1.getPropertyValue("recId") + "---"
//                            + item.f1.getPropertyValue("graphLabel") + "---" + item.f0.getSourceId() + "***" + item.f0.getTargetId());
//                System.out.println("##################################################################");
//            }
//        }
//if (t)
//    System.out.println(l_v_lId.size()+"  oo "+ s);
//        System.out.println("l_v_lId.size(): "+l_v_lId.size());

        for (Tuple3<Edge, Vertex, String> item:l_v_lId)
            collector.collect(item);
    }
}
