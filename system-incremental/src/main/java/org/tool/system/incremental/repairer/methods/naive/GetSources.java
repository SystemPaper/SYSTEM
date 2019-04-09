package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.List;

//in: fakeL_v_clsId.union(maxBoth_E_V_clsId).groupBy(2)
//out: maxBoth_E_V_srces
public class GetSources implements GroupReduceFunction <Tuple3<Edge,Vertex, String>, Tuple3<Edge, Vertex, String>> {
    @Override
    public void reduce(Iterable<Tuple3<Edge, Vertex, String>> iterable, Collector<Tuple3<Edge, Vertex, String>> collector) throws Exception {
//        Tuple2<Edge, Vertex> maxBoth = null;
//        List<Vertex> vertexList = new ArrayList<>();
//        List<String> vertexIdList = new ArrayList<>();
//
//        for (Tuple3<Edge, Vertex, String> it:iterable){
//            if (it.f0.getId() != null)
//                maxBoth = Tuple2.of(it.f0, it.f1);
//            else if (!vertexIdList.contains(it.f1.getId().toString())) {
//                vertexList.add(it.f1);
//                vertexIdList.add(it.f1.getId().toString());
//            }
//        }
//        if (maxBoth  != null){
//            String srces = "";
//            String src;
//            for (Vertex vertex: vertexList){
//                if (vertex.hasProperty("graphLabel"))
//                    src = vertex.getPropertyValue("graphLabel").toString();
//                else
//                    src = vertex.getPropertyValue("type").toString();
//
//                srces += (","+ src);
//            }
//            collector.collect(Tuple3.of(maxBoth.f0, maxBoth.f1, srces.substring(1)));
//        }
        Tuple2<Edge, Vertex> maxBoth = null;
        Double maxSim = -1d;
        List<Vertex> vertexList = new ArrayList<>();
        List<String> vertexIdList = new ArrayList<>();

        for (Tuple3<Edge, Vertex, String> it:iterable){
            if (it.f0.getId() != null) {
                Double sim = Double.parseDouble(it.f0.getPropertyValue("value").toString());
                Tuple2<Edge, Vertex> curMaxBoth = Tuple2.of(it.f0, it.f1);
                if (sim.compareTo(maxSim) > 0) {
                    maxBoth = curMaxBoth;
                    maxSim = sim;
                }
                else if (sim.equals(maxSim)){
                    if(curMaxBoth.f0.getId().compareTo(maxBoth.f0.getId()) > 0){
                        maxBoth = curMaxBoth;
                    }
                }
            }
            else if (!vertexIdList.contains(it.f1.getId().toString())) {
                vertexList.add(it.f1);
                vertexIdList.add(it.f1.getId().toString());
            }
        }
        if (maxBoth  != null){
            String srces = "";
            String src;
            for (Vertex vertex: vertexList){
                if (vertex.hasProperty("graphLabel"))
                    src = vertex.getPropertyValue("graphLabel").toString();
                else
                    src = vertex.getPropertyValue("type").toString();

                srces += (","+ src);
            }
            collector.collect(Tuple3.of(maxBoth.f0, maxBoth.f1, srces.substring(1)));
        }
    }
}
