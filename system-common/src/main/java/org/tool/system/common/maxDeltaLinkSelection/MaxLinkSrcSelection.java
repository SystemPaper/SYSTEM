package org.tool.system.common.maxDeltaLinkSelection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.common.maxDeltaLinkSelection.functions.FindMax2;
import org.tool.system.common.maxDeltaLinkSelection.functions.Link2Link_EndId_EndType;
import org.tool.system.common.maxDeltaLinkSelection.functions.MakeEdgeWithSelectedStatus2;
import org.tool.system.common.util.Link2Link_SrcVertex_TrgtVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 */
public class MaxLinkSrcSelection implements UnaryGraphToGraphOperator {
    private Double delta;

    @Override
    public String getName() {
        return "MaxLinkSrcSelection";
    }
    public MaxLinkSrcSelection(Double Delta) {delta = Delta;}

    @Override
    public LogicalGraph execute (LogicalGraph input) {

        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_targetVertex = new Link2Link_SrcVertex_TrgtVertex(input).execute();

        DataSet<Tuple3<Edge, String, String>> link_endId_endType = link_srcVertex_targetVertex.flatMap(new Link2Link_EndId_EndType());

        DataSet<Tuple3<Edge, String, Integer>> edges_edgeId_isSelected = link_endId_endType.groupBy(1,2).reduceGroup(new FindMax2(delta));
        DataSet<Edge> edges = edges_edgeId_isSelected.groupBy(1).reduceGroup(new MakeEdgeWithSelectedStatus2());
        return input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), edges);
    }
}
