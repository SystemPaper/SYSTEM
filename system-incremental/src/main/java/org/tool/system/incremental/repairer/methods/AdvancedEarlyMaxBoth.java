package org.tool.system.incremental.repairer.methods;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.clustering.parallelClustering.ClusteringGC;
import org.tool.system.clustering.parallelClustering.util.ModifyGraphforClustering;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPConfig;
import org.tool.system.common.functions.*;
import org.tool.system.common.util.Link2Link_SrcVertex_TrgtVertex;
import org.tool.system.incremental.repairer.methods.max_both.*;
import org.tool.system.incremental.repairer.methods.naive.*;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.tool.system.incremental.repairer.methods.ndr.GetUnclustredVertices;

public class AdvancedEarlyMaxBoth implements UnaryCollectionToCollectionOperator{
    private CLIPConfig clipConfig;
    private String prefix;
    private boolean preClustering = false;
    private boolean isSrcConsistent = true;


    public AdvancedEarlyMaxBoth(Integer srcNo, String prefix, Boolean preClustering, Boolean isSrcConsistent){
        clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(srcNo);
        this.prefix = prefix;
        this.preClustering = preClustering;
        this.isSrcConsistent = isSrcConsistent;
    }




    public GraphCollection execute(GraphCollection input)  {
        DataSet<Vertex> vertices;
        DataSet<Vertex> clusteredVertices;

        // clustering new entities

        if (preClustering) {
            DataSet<Vertex> unclusteredVertices =
                    input.getVertices().flatMap(new GetUnclustredVertices()).map(new AddNewProperty());
//            DataSet<Edge> unclusteredEdges =
//                    new Link2Link_SrcVertex_TrgtVertex(input.getVertices(), input.getEdges()).execute().flatMap(new GetNewNewLinks());
            DataSet<Edge> unclusteredEdges = input.getEdges().flatMap(new FlatMapFunction<Edge, Edge>() {
                @Override
                public void flatMap(Edge edge, Collector<Edge> collector) throws Exception {
                    if (edge.hasProperty("new"))
                        collector.collect(edge);
                }
            });

            GraphCollection clustered = input.getConfig().getGraphCollectionFactory().fromDataSets(
                    input.getGraphHeads(),unclusteredVertices, unclusteredEdges)
                    .callForCollection(new ClusteringGC(clipConfig, prefix));

            clusteredVertices = clustered.getVertices();

            vertices = input.getVertices().union(clusteredVertices).map(new Vertex2Vertex_GradoopId()).groupBy(1).reduceGroup(new RemoveUnclusteredVertices());
        }
        else {
            input = input.callForCollection(new ModifyGraphforClustering());
            DataSet<Tuple2<Vertex, Boolean>> vertex_isNew = input.getVertices().map(new AddClsIdNewPropRmvVP(prefix));

            clusteredVertices = vertex_isNew.flatMap(new GetNewVs());
            vertices = vertex_isNew.map(new GetF0Tuple2());
        }

        DataSet<Tuple3<Edge, Vertex, Vertex>> new_l_s_t =
                new Link2Link_SrcVertex_TrgtVertex(vertices, input.getEdges()).execute().flatMap(new GetInterNewOld());

        // max both

        DataSet<Tuple4<Edge, Vertex,String, String>> intE_s_sId_tSrc = new_l_s_t.flatMap(new GetIdOtherType());
        DataSet<Tuple3<Vertex, Vertex, Double>> maxBoth_v_newV = (intE_s_sId_tSrc.groupBy(2,3).reduceGroup(new GetMax())).groupBy(2)
                .reduceGroup(new AdvancedGetMaxBothVNewV());

        // find SRC-CONSISTENT

        DataSet<Tuple2<String, String>> clsId_srcs = vertices
                    .map(new Vertex2ClsIdSrc()).groupBy(0).reduceGroup(new GetClsIdSrces());
        DataSet<Tuple5<Vertex, Vertex, String, String, Double>> maxBothVPairsClsIds = maxBoth_v_newV.map(new AdvancedAddClsIds());
        DataSet<Tuple5<Vertex, Vertex, String, String, Double>> maxBothVPairsSrces = maxBothVPairsClsIds.join(clsId_srcs).where(2).equalTo(0)
                    .with(new AdvancedGetSrcJoin(2)).join(clsId_srcs).where(3).equalTo(0).with(new AdvancedGetSrcJoin(3));


        DataSet<Tuple5<Vertex, Vertex, String, String, Double>> maxBothSrcCnstnt_v_newV_srces = null;

        if (isSrcConsistent)
            maxBothSrcCnstnt_v_newV_srces = maxBothVPairsSrces.flatMap(new AdvancedGetSrcConsistentsVsSrcs());

        else
            maxBothSrcCnstnt_v_newV_srces = maxBothVPairsSrces;


        // remove the situation of multi new items to one old item and vise versa
        DataSet<Tuple6<Vertex, Vertex, String, String, String, Double>> maxBothSrcCnstnt_v_newV_srces_vClsId = maxBothSrcCnstnt_v_newV_srces.map(new AdvancedAddClsId1(0));
        maxBothSrcCnstnt_v_newV_srces = maxBothSrcCnstnt_v_newV_srces_vClsId.groupBy(4).reduceGroup(new AdvancedGetMergableOnes(0));
        maxBothSrcCnstnt_v_newV_srces_vClsId = maxBothSrcCnstnt_v_newV_srces.map(new AdvancedAddClsId1(1));
        maxBothSrcCnstnt_v_newV_srces = maxBothSrcCnstnt_v_newV_srces_vClsId.groupBy(4).reduceGroup(new AdvancedGetMergableOnes(1));
        DataSet<Tuple2<Vertex, Vertex>> maxBothSrcCnstnt_v_newV = maxBothSrcCnstnt_v_newV_srces.map(new GetF0F1Tuple5());




        // merge SRC-CONSISTENT clusters

        DataSet<Tuple2<String, String>> oldClsId_newClsId = maxBothSrcCnstnt_v_newV.map(new GetClsIds());
        DataSet<Tuple2<Vertex, String>> newV_clsId = clusteredVertices.flatMap(new Vertex2Vertex_ClusterId(false));
        DataSet<Vertex> mergedVertices = newV_clsId.join(oldClsId_newClsId).where(1).equalTo(1).with(new JoinMergeCluster());



        vertices = vertices.union(mergedVertices).map(new Vertex2Vertex_GradoopId()).groupBy(1).reduceGroup(new RemoveDoubles())
        .map(new RemoveExtraProperty());

        return input.getConfig().getGraphCollectionFactory().fromDataSets(input.getGraphHeads(), vertices);

    }


    @Override
    public String getName() {
        return getClass().getName();
    }



}























