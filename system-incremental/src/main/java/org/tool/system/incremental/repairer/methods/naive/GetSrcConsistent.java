package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Arrays;
import java.util.List;

public class GetSrcConsistent implements GroupReduceFunction <Tuple4<Edge, Vertex, String, String>,Tuple3<Edge, Vertex, Vertex>> {
    @Override
    public void reduce(Iterable<Tuple4<Edge, Vertex, String, String>> iterable, Collector<Tuple3<Edge, Vertex, Vertex>> collector) throws Exception {
        String srces1 = "";
        String srces2 = "";
        Vertex v1 = null;
        Vertex v2 = null;
        Edge e = null;

        for (Tuple4<Edge, Vertex, String, String> it: iterable){
            if (srces1.equals("")) {
                srces1 = it.f2;
                v1 = it.f1;
                e = it.f0;
            }
            else {
                srces2 = it.f2;
                v2 = it.f1;
            }

        }
        if (IsConsistent(srces1,srces2) && v2!= null)
            collector.collect(Tuple3.of(e,v1,v2));
    }

    private boolean IsConsistent(String srces1, String srces2) {
        List<String> srcList1 = Arrays.asList(srces1.split(","));
        List<String> srcList2 = Arrays.asList(srces2.split(","));
        for (String src:srcList2){
            if (srcList1.contains(src))
                return false;
        }
        return true;
    }
}
