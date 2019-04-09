//package org.gradoop.famer.incremental.repairer.methods.max_both;
//
//import org.apache.flink.api.common.functions.GroupReduceFunction;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.util.Collector;
//import org.gradoop.common.model.impl.pojo.Vertex;
//
//public class AdvancedGetMaxBothVNewVSrcs implements GroupReduceFunction<Tuple4<String, Vertex, String, Double>,
//        Tuple4<Vertex, Vertex, String, String, Double>> {
//    @Override
//    public void reduce(Iterable<Tuple4<String, Vertex, String, Double>> iterable, Collector<Tuple4<Vertex, Vertex, String, String>> collector) throws Exception {
//        Vertex v1 = null;
//        Vertex v2 = null;
//        String srcs1 = "";
//        String srcs2 = "";
//
//        for (Tuple3<String, Vertex, String> it:iterable){
//            if (v1==null) {
//                v1 = it.f1;
//                srcs1 = it.f2;
//            }
//            else {
//                v2 = it.f1;
//                srcs2 = it.f2;
//            }
//        }
//
//        if (v2 != null) {
//
//            if(v1.hasProperty("new"))
//                collector.collect(Tuple4.of(v2, v1, srcs2, srcs1));
//            else
//                collector.collect(Tuple4.of(v1, v2, srcs1, srcs2));
//
//        }
//    }
//}
package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AdvancedGetMaxBothVNewVSrcs implements GroupReduceFunction<Tuple4<String, Vertex, String, Double>,
        Tuple5<Vertex, Vertex, String, String, Double>> {
    @Override
    public void reduce(Iterable<Tuple4<String, Vertex, String, Double>> iterable,
                       Collector<Tuple5<Vertex, Vertex, String, String, Double>> collector) throws Exception {
        Vertex v1 = null;
        Vertex v2 = null;
        String srcs1 = "";
        String srcs2 = "";
        double sim = 0d;
        for (Tuple4<String, Vertex, String, Double> it:iterable){
            if (v1==null) {
                v1 = it.f1;
                srcs1 = it.f2;
            }
            else {
                v2 = it.f1;
                srcs2 = it.f2;
            }
            sim = it.f3;
        }

        if (v2 != null) {

            if(v1.hasProperty("new"))
                collector.collect(Tuple5.of(v2, v1, srcs2, srcs1,sim));
            else
                collector.collect(Tuple5.of(v1, v2, srcs1, srcs2,sim));

        }
    }
}
