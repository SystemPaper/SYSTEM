package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.tool.system.common.functions.Link2Link_SrcId_TrgtId;

/**
 *
 */

//DataSet<Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>>> link_srcVertex_targetVertex =
//        new Link2Link_SrcVertex_TrgtVertex2(Vid_src, input.getEdges()).execute();

public class Link2Link_SrcVertex_TrgtVertex2 {
    private DataSet<Tuple2<String, String>> vertices;
    private DataSet<Edge> edges;
    public Link2Link_SrcVertex_TrgtVertex2(DataSet<Tuple2<String, String>> vertices, DataSet<Edge> edges){
        this.vertices = vertices;
        this.edges = edges;
    }

    public DataSet<Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>>> execute (){

        DataSet<Tuple3<Edge, String, String>> edge_srcId_trgtId = edges.map(new Link2Link_SrcId_TrgtId());
        DataSet<Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>>> edge_srcVertex_trgtVertex =
                edge_srcId_trgtId.join(vertices).where(1).equalTo(0).
                with(new Join1())
                .join(vertices).where(2).equalTo(0).with(new Join2());
        return edge_srcVertex_trgtVertex;
    }



}
