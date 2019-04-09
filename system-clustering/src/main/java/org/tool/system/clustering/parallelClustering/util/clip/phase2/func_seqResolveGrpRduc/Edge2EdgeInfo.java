package org.tool.system.clustering.parallelClustering.util.clip.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 */
public class Edge2EdgeInfo implements MapFunction <Edge, Tuple4<String, String, String, Double>>{
    private Double simValueCoef;
    private Double strengthCoef;

    public Edge2EdgeInfo(Double inputSimValueCoef, Double inputStrengthCoef){
        simValueCoef = inputSimValueCoef;
        strengthCoef = inputStrengthCoef;
    }

    @Override
    public Tuple4<String, String, String, Double> map(Edge edge) throws Exception {
        Double prioValue = simValueCoef*Double.parseDouble(edge.getPropertyValue("value").toString())
                +strengthCoef*Integer.parseInt(edge.getPropertyValue("isSelected").toString());
        return Tuple4.of(edge.getId().toString(), edge.getSourceId().toString(), edge.getTargetId().toString(), prioValue);
    }
}
