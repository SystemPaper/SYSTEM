package org.tool.system.linking.linking.NAIC;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class MinusBlocking {
    private DataSet<Tuple2<Vertex, Vertex>> blockedVertices;
    private DataSet<Edge> currentEdges;
    public MinusBlocking(DataSet<Tuple2<Vertex, Vertex>> blockedVertices, DataSet<Edge> currentEdges){
        this.blockedVertices = blockedVertices;
        this.currentEdges = currentEdges;
    }
    public DataSet<Tuple2<Vertex, Vertex>> execute(){
        DataSet<Tuple3<Vertex, Vertex, String>> blockedVerticesIds = blockedVertices.map(new GetVertexTupleIds());
        DataSet<Tuple3<Vertex, Vertex, String>> edgesIds = currentEdges.map(new GetEdgeEndsIds());
        return blockedVerticesIds.union(edgesIds).groupBy(2).reduceGroup(new GetMinus());
    }

    private class GetVertexTupleIds implements MapFunction<Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, String>> {
        @Override
        public Tuple3<Vertex, Vertex, String> map(Tuple2<Vertex, Vertex> in) throws Exception {
            String id;
            if(in.f0.getId().compareTo(in.f1.getId()) < 0)
                id = in.f0.toString()+in.f1.getId().toString();
            else
                id = in.f1.toString()+in.f0.getId().toString();

            return Tuple3.of(in.f0, in.f1, id);
        }
    }

    private class GetEdgeEndsIds implements MapFunction<Edge, Tuple3<Vertex, Vertex, String>> {
        @Override
        public Tuple3<Vertex, Vertex, String> map(Edge in) throws Exception {
            String id;
            if(in.getSourceId().compareTo(in.getTargetId()) < 0)
                id = in.getSourceId().toString()+in.getTargetId().toString();
            else
                id = in.getTargetId().toString()+in.getSourceId().toString();
            Vertex v= new Vertex();
            return Tuple3.of(v,v,id);
        }
    }

    private class GetMinus implements GroupReduceFunction<Tuple3<Vertex, Vertex, String>, Tuple2<Vertex, Vertex>> {
        @Override
        public void reduce(Iterable<Tuple3<Vertex, Vertex, String>> iterable, Collector<Tuple2<Vertex, Vertex>> collector) throws Exception {
            int i = 0;
            Tuple2<Vertex, Vertex> pair = null;
            for (Tuple3<Vertex, Vertex, String> it: iterable){
                i ++;
                pair = Tuple2.of(it.f0, it.f1);
            }
            if (i==1 && pair.f0.getId()!= null)
                collector.collect(pair);
        }
    }
}

















