package org.tool.system.incremental.representator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.common.functions.*;
import org.tool.system.incremental.representator.func.*;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class Expand implements UnaryCollectionToCollectionOperator {
    @Override
    public GraphCollection execute(GraphCollection input) {

        GradoopFlinkConfig config = input.getConfig();

        DataSet<Tuple4<Vertex, String, String, String>> vertex_newId_oldId_src =
                input.getVertices().flatMap(new SplitSummarizedVertices(config.getVertexFactory(), "graphLabel"));

        DataSet<Tuple3<Edge, String, String>> edge_srcId_trgtId = input.getEdges().map(new Link2Link_SrcId_TrgtId());

        edge_srcId_trgtId = edge_srcId_trgtId.map(new ModiFyLoopSimValues());

        // reduce edges by produce the average from multiple edges between same ends
        edge_srcId_trgtId = edge_srcId_trgtId.map(new OrderEndIds()).groupBy(1, 2).reduceGroup(new GetAverage());

        DataSet<Tuple3<String, String, String>> vNewId_vOldId_src = vertex_newId_oldId_src.map(new RemoveF0Tuple4());

        DataSet<Tuple3<Edge, String, String>> edge_trgtId_srcSrc =
                vNewId_vOldId_src.join(edge_srcId_trgtId).where(1).equalTo(1).with(new ExpandJoin1());


        DataSet<Tuple2<Edge, Boolean>> edge_isCorrect =
                edge_trgtId_srcSrc.join(vNewId_vOldId_src).where(1).equalTo(1).with(new ExpandJoin2());
        DataSet<Edge> edges = edge_isCorrect.flatMap(new GetCorrectEdges());
        edges = edges.map(new GetEdgeconcatIds()).distinct(1).map(new GetF0Tuple2());
        DataSet<Vertex> vertices = vertex_newId_oldId_src.map(new GetF0Tuple4());

        edges = edges.map(new MapFunction<Edge, Edge>() {
            @Override
            public Edge map(Edge edge) throws Exception {
                if(edge.getSourceId().compareTo(edge.getTargetId()) >= 0) {
                    GradoopId sourceId = edge.getSourceId();
                    GradoopId targetId = edge.getTargetId();
                    edge.setSourceId(targetId);
                    edge.setTargetId(sourceId);
                }
                return edge;
            }
        }).map(new Link2Link_Id()).distinct(1).map(new GetF0Tuple2());

        vertices= vertices.map(new Vertex2Vertex_GradoopId()).distinct(1).map(new GetF0Tuple2<>());

        return  config.getGraphCollectionFactory().fromGraph(
                config.getLogicalGraphFactory().fromDataSets(vertices, edges));
    }

    @Override
    public String getName() {
        return this.getName();
    }
}
