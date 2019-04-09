package org.tool.system.incremental.repairer.methods;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.clustering.parallelClustering.ClusteringGraph;
import org.tool.system.clustering.parallelClustering.util.ClusteringOutputType;
import org.tool.system.clustering.parallelClustering.util.ModifyGraphforClustering;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPConfig;
import org.tool.system.common.functions.GetF0Tuple2;
import org.tool.system.common.functions.Vertex2Vertex_ClusterId;
import org.tool.system.common.functions.Vertex2Vertex_GradoopId;
import org.tool.system.common.util.Link2Link_SrcVertex_TrgtVertex;
import org.tool.system.incremental.repairer.methods.max_both.*;
import org.tool.system.incremental.repairer.methods.naive.AddClsIdNewPropRmvVP;
import org.tool.system.incremental.repairer.methods.naive.AddNewProperty;
import org.tool.system.incremental.repairer.methods.naive.GetNewVs;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.tool.system.incremental.repairer.methods.ndr.GetUnclustredVertices;

public class AdvancedLateMaxBoth implements UnaryCollectionToCollectionOperator{
    private CLIPConfig clipConfig;
    private String prefix;
    private boolean preClustering = false;


    public AdvancedLateMaxBoth(Integer srcNo, String prefix, Boolean preClustering){
        clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(srcNo);
        this.prefix = prefix;
        this.preClustering = preClustering;
    }




    public GraphCollection execute(GraphCollection input)  {
        DataSet<Vertex> vertices;
        DataSet<Vertex> clusteredVertices;

        // clustering new entities

        if (preClustering) {
            DataSet<Vertex> unclusteredVertices = input.getVertices().flatMap(new GetUnclustredVertices()).map(new AddNewProperty());
            DataSet<Edge> unclusteredEdges =
                    new Link2Link_SrcVertex_TrgtVertex(input.getVertices(), input.getEdges()).execute().flatMap(new GetNewNewLinks());

            LogicalGraph clustered = input.getConfig().getLogicalGraphFactory().fromDataSets(unclusteredVertices, unclusteredEdges)
                    .callForGraph(new ClusteringGraph(clipConfig, prefix, ClusteringOutputType.GRAPH));

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



        // find SRC-CONSISTENT

        DataSet<Tuple2<String, String>> clsId_srcs = vertices
                .map(new Vertex2ClsIdSrc()).groupBy(0).reduceGroup(new GetClsIdSrces());
        DataSet<Tuple6<String, Double, Vertex, Vertex, String, String>> lId_sim_v_newV_clsId_newClsId =  new_l_s_t.map(new GetLinkSpecVnewVClsIds());
        DataSet<Tuple6<String, Double, Vertex, Vertex, String, String>> lId_sim_v_newV_sSrc_tSrc =
                lId_sim_v_newV_clsId_newClsId.join(clsId_srcs).where(4).equalTo(0)
                        .with(new AddSrcJoin(2)).join(clsId_srcs).where(5).equalTo(0).with(new AddSrcJoin(3));
        DataSet<Tuple6<String, Double, Vertex, Vertex, String, String>> srcCnstnt_lId_sim_v_newV_sSrc_tSrc =
                lId_sim_v_newV_sSrc_tSrc.flatMap(new GetSrcConsistentItems());

        // max both

        DataSet<Tuple6<String, Double, Vertex, String, String, String>> lId_sim_v_src_id_otherType =
                srcCnstnt_lId_sim_v_newV_sSrc_tSrc.flatMap(new AddIdOtherType());
        DataSet<Tuple5<Vertex, Vertex, String, String, Double>> maxBothSrcCnstnt_v_newV_srces = lId_sim_v_src_id_otherType.groupBy(4,5)
                .reduceGroup(new AdvancedGetMaxPairs()).groupBy(0)
                .reduceGroup(new AdvancedGetMaxBothVNewVSrcs());

        // remove the situation of multi new items to one old item and vise versa
        DataSet<Tuple6<Vertex, Vertex, String, String, String, Double>> maxBothSrcCnstnt_v_newV_srces_vClsId = maxBothSrcCnstnt_v_newV_srces.map(new AdvancedAddClsId1(0));
        maxBothSrcCnstnt_v_newV_srces = maxBothSrcCnstnt_v_newV_srces_vClsId.groupBy(4).reduceGroup(new AdvancedGetMergableOnes(0));
        maxBothSrcCnstnt_v_newV_srces_vClsId = maxBothSrcCnstnt_v_newV_srces.map(new AdvancedAddClsId1(1));
        DataSet<Tuple2<Vertex, Vertex>> maxBothSrcCnstnt_v_newV =  maxBothSrcCnstnt_v_newV_srces_vClsId.groupBy(4).reduceGroup(new GroupReduceFunction<Tuple6<Vertex, Vertex, String, String, String, Double>, Tuple2<Vertex, Vertex>>() {
            @Override
            public void reduce(Iterable<Tuple6<Vertex, Vertex, String, String, String, Double>> iterable,
                               Collector<Tuple2<Vertex, Vertex>> collector) throws Exception {
                double maxSim = 0d;
                double maxSize = 0;
                Tuple2<Vertex, Vertex> out = null;
//                maxBothSrcCnstnt_v_newV_srces_newvClsId
                for (Tuple6<Vertex, Vertex, String, String, String, Double> it:iterable){
                    double currentSim = it.f5;
                    double currentSize = it.f2.split(",").length;
                    if (currentSim > maxSim || (currentSim == maxSim &&  currentSize > maxSize)){
                        maxSim = currentSim;
                        maxSize = currentSize;
                        out = Tuple2.of(it.f0, it.f1);
                    }
                }
                collector.collect(out);

            }
        });
        // merge SRC-CONSISTENT clusters

        DataSet<Tuple2<String, String>> oldClsId_newClsId = maxBothSrcCnstnt_v_newV.map(new GetClsIds());
        DataSet<Tuple2<Vertex, String>> newV_clsId = clusteredVertices.flatMap(new Vertex2Vertex_ClusterId(false));
        DataSet<Vertex> mergedVertices = newV_clsId.join(oldClsId_newClsId).where(1).equalTo(1).with(new JoinMergeCluster());


        //output
        vertices = vertices.union(mergedVertices).map(new Vertex2Vertex_GradoopId()).groupBy(1).reduceGroup(new RemoveDoubles())
                .map(new RemoveExtraProperty());


        return input.getConfig().getGraphCollectionFactory().fromDataSets(input.getGraphHeads(), vertices);

    }


    @Override
    public String getName() {
        return getClass().getName();
    }



}























