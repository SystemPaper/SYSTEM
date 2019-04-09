package org.tool.system.incremental.repairer.methods;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.clustering.parallelClustering.ClusteringGraph;
import org.tool.system.clustering.parallelClustering.util.ClusteringOutputType;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPConfig;
import org.tool.system.common.functions.*;
import org.tool.system.common.util.Link2Link_SrcVertex_TrgtVertex;
import org.tool.system.common.util.Minus;
import org.tool.system.incremental.repairer.methods.ndr.*;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class

NDepthReclustering implements UnaryGraphToGraphOperator, UnaryCollectionToCollectionOperator {


    private CLIPConfig clipConfig;
    private GradoopFlinkConfig config;
    private Integer depth;
    private String other;
    // temporary: we can define a ProgressiveRepairerConfig later

    public NDepthReclustering(Integer srcNo, Integer depth, GradoopFlinkConfig config){
        clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(srcNo);
        this.config = config;
        this.depth = depth;
        other="";
    }
    public NDepthReclustering(Integer srcNo, Integer depth, String clipPrefix, GradoopFlinkConfig config){
        clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(srcNo);
        this.config = config;
        this.depth = depth;
        other = clipPrefix;
    }



//    private LogicalGraph repair(DataSet<Vertex> vertices, DataSet<Edge> edges) {

    private LogicalGraph repair(DataSet<Vertex> vertices, DataSet<Edge> edges) {


        DataSet<Tuple3<Edge, Vertex, Vertex>> link_srcVertex_targetVertex = new Link2Link_SrcVertex_TrgtVertex(vertices, edges).execute();
        DataSet<Tuple2<String, String>> cls1_cls2 = link_srcVertex_targetVertex.map(new GetF1F2Tuple3()).flatMap(new GetclusterNeighbors());
        // distinct
        cls1_cls2 = cls1_cls2.map(new Concat()).distinct(0).map(new Split());
        DataSet<Tuple1<String>> curObjClsIds = config.getExecutionEnvironment().fromElements(Tuple1.of(""));
        DataSet<Tuple1<String>> allObjClsIds = curObjClsIds;
        do {
            // neighbors of objClsIds
            DataSet<Tuple2<String, String>> objClsId_fake = curObjClsIds.map(new MakeCurTuple2());
            DataSet<Tuple1<String>> prevObjClsIds = curObjClsIds;
            curObjClsIds = cls1_cls2.union(objClsId_fake).groupBy(0).reduceGroup(new GetObjNeighbors()).distinct(0);
            allObjClsIds = allObjClsIds.union(curObjClsIds).distinct(0);
            cls1_cls2 = new MinusClsNeibors(cls1_cls2, prevObjClsIds).execute();
            depth --;
        } while (depth > 0);


        DataSet<Tuple3<Edge,Vertex, String>> link_st_clsId = link_srcVertex_targetVertex.flatMap(new GetLink_ST_ClsId());
        DataSet<Tuple3<Edge,Vertex, String>> fakeL_fakeST_clsId = allObjClsIds.map(new MakeAllTuple3());
        DataSet<Tuple2<Edge, Vertex>> link_vertex = link_st_clsId.union(fakeL_fakeST_clsId).groupBy(2).reduceGroup(new GetIntersection());
        DataSet<Edge> GPEdges = link_vertex.map(new GetF0Tuple2()).map(new Link2Link_Id()).distinct(1).map(new GetF0Tuple2());
        //inorder to add singletons
        DataSet<Vertex> unclusteredV = vertices.flatMap(new GetUnclustredVertices());

        DataSet<Vertex> GPVertices = link_vertex.map(new GetF1Tuple2()).map(new Vertex2Vertex_GradoopId())
                .union(unclusteredV.map(new Vertex2Vertex_GradoopId()))
                .distinct(1).map(new GetF0Tuple2());

        LogicalGraph reclusteredG = config.getLogicalGraphFactory().fromDataSets(GPVertices, GPEdges).callForGraph(new ClusteringGraph(clipConfig, other, ClusteringOutputType.GRAPH));
        DataSet<Vertex> reclusteredVs = reclusteredG.getVertices();
        DataSet<Edge> reclusteredEs = reclusteredG.getEdges();


        DataSet<Vertex> unchangedVs = new Minus(vertices.map(new Vertex2Vertex_GradoopId()), reclusteredVs.map(new Vertex2Vertex_GradoopId())).execute();
        DataSet<Edge> unchangedEs = new Minus(edges.map(new Link2Link_Id()), GPEdges.map(new Link2Link_Id())).execute();

        return config.getLogicalGraphFactory().fromDataSets(unchangedVs.union(reclusteredVs), unchangedEs.union(reclusteredEs));

    }

    @Override
    public String getName() {
        return getClass().getName();
    }

    @Override
    public GraphCollection execute(GraphCollection graphCollection) {
        return config.getGraphCollectionFactory().fromGraph(repair(graphCollection.getVertices(), graphCollection.getEdges()));
    }
    @Override
    public LogicalGraph execute(LogicalGraph graph) {
        return repair(graph.getVertices(), graph.getEdges());
    }

}
































