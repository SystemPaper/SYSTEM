package org.tool.system.clustering.parallelClustering;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

import org.tool.system.clustering.parallelClustering.util.clip.functions.AddFakeEdge;
import org.tool.system.clustering.parallelClustering.util.clip.functions.FilterSrcConsistentClusterVertices;
import org.tool.system.clustering.parallelClustering.util.clip.functions.Link2Link_EndId;
import org.tool.system.clustering.parallelClustering.util.clip.phase2.ResolveGrpRduc2;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPConfig;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPMinus;
import org.tool.system.common.functions.FilterOutSpecificLinks;
import org.tool.system.common.functions.GetF0Tuple2;
import org.tool.system.common.functions.Vertex2Vertex_ClusterId;
import org.tool.system.common.functions.Vertex2Vertex_GradoopId;
import org.tool.system.common.maxDeltaLinkSelection.functions.FindMax2;
import org.tool.system.common.maxDeltaLinkSelection.functions.Link2Link_EndId_EndType;
import org.tool.system.common.maxDeltaLinkSelection.functions.MakeEdgeWithSelectedStatus2;

import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.tool.system.common.util.*;

/**
 */
public class ClusteringGC implements UnaryCollectionToCollectionOperator{
    private CLIPConfig clipConfig;
    private String prefix;


    public ClusteringGC(CLIPConfig clipConfig){
        this.clipConfig = clipConfig;
        this.prefix = "";
    }
    public ClusteringGC(CLIPConfig clipConfig, String prefix){
        this.clipConfig = clipConfig;
        this.prefix = prefix;
    }


    @Override
    public GraphCollection execute(GraphCollection input) {
//        try {
//            System.out.println(input.getVertices().count()+"&&&&&&&&");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_targetVertex = new Link2Link_SrcVertex_TrgtVertex(input.getVertices(), input.getEdges()).execute();

        DataSet<Tuple3<Edge, String, String>> link_endId_endType = link_srcVertex_targetVertex.flatMap(new Link2Link_EndId_EndType());

        DataSet<Tuple3<Edge, String, Integer>> edges_edgeId_isSelected = link_endId_endType.groupBy(1,2).reduceGroup(new FindMax2(clipConfig.getDelta()));
        DataSet<Edge> allEdgesWithSelectedStatus = edges_edgeId_isSelected.groupBy(1).reduceGroup(new MakeEdgeWithSelectedStatus2());

        DataSet<Edge> strongEdges = allEdgesWithSelectedStatus.flatMap(new FilterOutSpecificLinks(1));

        // remove vertices and edges of complete clusters
        LogicalGraph temp = input.getConfig().getLogicalGraphFactory().fromDataSets(input.getVertices(), strongEdges);
        temp = temp.callForGraph(new ConnectedComponents(prefix+"ph1-"));
        DataSet<Tuple2<Vertex, String>> completeVerticesWithId = temp.getVertices().flatMap(new Vertex2Vertex_ClusterId(false)).groupBy(1)
                .reduceGroup(new FilterSrcConsistentClusterVertices(clipConfig.getSourceNo())).map(new Vertex2Vertex_GradoopId());

        // remove complete vertices
        DataSet<Tuple2<Vertex, String>> allVerticesWithId = input.getVertices().map(new Vertex2Vertex_GradoopId());
        DataSet<Vertex> remainingVertices = new Minus(allVerticesWithId, completeVerticesWithId).execute();

        // remove edges of complete clusters or related to complete clusters
        DataSet<Tuple2<Edge,String>> completeVerticesIds_FakeEdge = completeVerticesWithId.map(new AddFakeEdge());

        DataSet<Tuple2<Edge, String>> allNonWeakEdgesWithSrcId = (allEdgesWithSelectedStatus.flatMap(new FilterOutSpecificLinks(0))).map(new Link2Link_EndId(0));
        DataSet<Edge> NonWeakEdges_temp = new CLIPMinus().execute(allNonWeakEdgesWithSrcId, completeVerticesIds_FakeEdge);
        DataSet<Tuple2<Edge, String>> NonWeakEdges_tempWithTrgtId = NonWeakEdges_temp.map(new Link2Link_EndId(1));
        DataSet<Edge> remainingEdges = new CLIPMinus().execute(NonWeakEdges_tempWithTrgtId, completeVerticesIds_FakeEdge);
        temp = input.getConfig().getLogicalGraphFactory().fromDataSets(remainingVertices, remainingEdges);


        /* if (isRemoveSrcConsistentVertices:true)
            Phase 2 is added
            Phase 2: All source consistent clusters smaller than srcNo size are removed from the graph.
        */
        /////////////////////////////////////
        if (clipConfig.isRemoveSrcConsistentVertices()) {

            temp = temp.callForGraph(new ConnectedComponents(prefix+"ph2-"));
            DataSet<Tuple2<Vertex, String>> srcConsistentVerticesWithId = temp.getVertices().flatMap(new Vertex2Vertex_ClusterId(false)).groupBy(1)
                    .reduceGroup(new FilterSrcConsistentClusterVertices()).map(new Vertex2Vertex_GradoopId());
            allVerticesWithId = temp.getVertices().map(new Vertex2Vertex_GradoopId());
            remainingVertices = new Minus(allVerticesWithId, srcConsistentVerticesWithId).execute();
            DataSet<Tuple2<Edge, String>> srcConsistentVerticesIds_FakeEdge = srcConsistentVerticesWithId.map(new AddFakeEdge());
            allNonWeakEdgesWithSrcId = remainingEdges.map(new Link2Link_EndId(0));
            NonWeakEdges_temp = new CLIPMinus().execute(allNonWeakEdgesWithSrcId, srcConsistentVerticesIds_FakeEdge);
            NonWeakEdges_tempWithTrgtId = NonWeakEdges_temp.map(new Link2Link_EndId(1));
            remainingEdges = new CLIPMinus().execute(NonWeakEdges_tempWithTrgtId, srcConsistentVerticesIds_FakeEdge);

            completeVerticesWithId = completeVerticesWithId.union(srcConsistentVerticesWithId);
            temp = input.getConfig().getLogicalGraphFactory().fromDataSets(remainingVertices, remainingEdges);
        }
        /////////////////////////////////////


        temp = temp.callForGraph(new ResolveGrpRduc2(clipConfig.getSimValueCoef(), clipConfig.getStrengthCoef()));
        temp = temp.callForGraph(new ConnectedComponents(prefix+"ph3-"));

        input = input.getConfig().getGraphCollectionFactory().fromDataSets(input.getGraphHeads()
                , temp.getVertices().union(completeVerticesWithId.map(new GetF0Tuple2()))
                , input.getEdges());


        return input;
    }

    @Override
    public String getName() {
        return ClusteringGC.class.getName();
    }


}
