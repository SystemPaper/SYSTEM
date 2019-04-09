package org.tool.system.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.common.functions.Link2Link_SrcId_TrgtId;
import org.tool.system.common.functions.Vertex2Vertex_GradoopId;
import org.tool.system.common.util.functions.gradoopId2vertexJoin1;
import org.tool.system.common.util.functions.gradoopId2vertexJoin2;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 *
 */
public class Link2Link_SrcVertex_TrgtVertex {
    private DataSet<Vertex> vertices;
    private DataSet<Edge> edges;
    public Link2Link_SrcVertex_TrgtVertex(LogicalGraph Graph){
        vertices = Graph.getVertices();
        edges = Graph.getEdges();
    }
    public Link2Link_SrcVertex_TrgtVertex(DataSet<Vertex> Vertices, DataSet<Edge> Edges){
        vertices = Vertices;
        edges = Edges;
    }
    public DataSet<Tuple3<Edge, Vertex, Vertex>> execute (){
        DataSet<Tuple2<Vertex, String>> vertex_gradoopId = vertices.map(new Vertex2Vertex_GradoopId());
        DataSet<Tuple3<Edge, String, String>> edge_srcId_trgtId = edges.map(new Link2Link_SrcId_TrgtId());
        DataSet<Tuple3<Edge, Vertex, Vertex>> edge_srcVertex_trgtVertex = edge_srcId_trgtId.join(vertex_gradoopId).where(1).equalTo(1).
                with(new gradoopId2vertexJoin1())
                .join(vertex_gradoopId).where(2).equalTo(1).with(new gradoopId2vertexJoin2());
        return edge_srcVertex_trgtVertex;
    }



}
