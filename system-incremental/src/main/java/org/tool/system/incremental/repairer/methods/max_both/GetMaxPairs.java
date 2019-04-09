package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.List;

// input: srcCnstnt_lId_sim_v_newV_sSrc_tSrc_id_otherType
public class GetMaxPairs implements GroupReduceFunction <Tuple6<String, Double, Vertex, String, String, String>
        , Tuple3<String, Vertex, String>> {
    @Override
    public void reduce(Iterable<Tuple6<String, Double, Vertex, String, String, String>> iterable,
                       Collector<Tuple3<String, Vertex, String>> collector) throws Exception {
        List<Tuple3<String, Vertex, String>> lId_v_vSrces = new ArrayList();
        Double maxSim = -1d;
        for (Tuple6<String, Double, Vertex, String, String, String> it : iterable) {

            Double sim = it.f1;
//            if (sim > maxSim){
            if (sim.compareTo(maxSim) > 0) {
                maxSim = sim;
                lId_v_vSrces.clear();
                lId_v_vSrces.add(Tuple3.of(it.f0, it.f2, it.f3));
            } else if (sim.equals(maxSim)) {
                lId_v_vSrces.add(Tuple3.of(it.f0, it.f2, it.f3));
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

        for (Tuple3<String, Vertex, String> item : lId_v_vSrces)
            collector.collect(item);
    }
}
