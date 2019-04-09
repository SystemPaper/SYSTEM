package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.gradoop.common.model.impl.pojo.Edge;
import org.tool.system.clustering.parallelClustering.util.clip.phase2.func_seqResolveGrpRduc.*;
import org.tool.system.clustering.parallelClustering.util.clip.phase2.func_seqResolveGrpRduc.Join2;
import org.tool.system.common.functions.Convert2Tuple1;
import org.tool.system.common.functions.Link2Link_Id;
import org.tool.system.clustering.parallelClustering.util.clip.phase2.func_seqResolveGrpRduc.*;

/**
 */
public class ResolveGrpRduc3 {
    private Double simValueCoef;
    private Double strengthCoef;
    private DataSet<Tuple2<String, String>> vertices;
    private DataSet<Edge> edges;
    private String prefix;
    public ResolveGrpRduc3(Double inputSimValueCoef, Double inputStrengthCoef, DataSet<Tuple2<String, String>> vertices,
            DataSet<Edge> edges, String prefix){
        simValueCoef = inputSimValueCoef;
        strengthCoef = inputStrengthCoef;
        this.vertices = vertices;
        this.edges = edges;
        this.prefix = prefix;
    }
    public DataSet<Tuple3<String, String, String>> execute() throws Exception {
        DataSet<Tuple3<String, String, String>> vertices_clsId = null;
        try {
            vertices_clsId = new ConnectedComponents_2(vertices, edges, "").execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


        DataSet<Tuple4<String, String, String, Double>> edgeId_srcId_trgtId_prioValue =
                edges.map(new Edge2EdgeInfo(simValueCoef, strengthCoef));
        DataSet<Tuple5<String, String, String, String, Double>> join1Result =
                vertices_clsId.join(edgeId_srcId_trgtId_prioValue).where(0).equalTo(1).
                with(new org.tool.system.clustering.parallelClustering.util.clip.phase2.func_seqResolveGrpRduc.Join1());
        DataSet<Tuple7<String, String, String, String, String, String, Double>> srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_prioValue
                = join1Result.join(vertices_clsId).where(3).equalTo(0).with(new Join2());
        DataSet<Tuple1<String>> edgeIds = srcSrc_trgtSrc_conComId_edgeId_srcId_trgtId_prioValue.groupBy(2).sortGroup(6, Order.DESCENDING)
                .reduceGroup(new Reducer()).map(new Convert2Tuple1());
        DataSet<Tuple2<Edge, String>> edge_edgeId = edges.map(new Link2Link_Id());
        DataSet<Edge> edges = edge_edgeId.join(edgeIds).where(1).equalTo(0).with(new Join());
        DataSet<Tuple3<String, String, String>> output =
                new ConnectedComponents_2(vertices, edges, prefix).execute();
        return output;
    }


}
