//package org.tool.system.incremental.repairer.methods.initialDepth;
//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.util.Collector;
//import org.gradoop.common.model.impl.pojo.Edge;
//import org.gradoop.common.model.impl.pojo.Vertex;
//
//public class GetCLIPInput implements FlatMapFunction<Tuple3<Edge, Vertex, Vertex>, Tuple3<Edge, Vertex, Vertex>> {
//    @Override
//    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple3<Edge, Vertex, Vertex>> collector) throws Exception {
//        if(!in.f1.hasProperty("ClusterId") && in.f2.hasProperty("ClusterId"))
//
//    }
//}
