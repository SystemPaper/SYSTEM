package org.tool.system.clustering.parallelClustering.util.clip.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
//DataSet<Tuple2<String, String>> completeVertices_id_clsId = tempVertices_clsId.groupBy(2)
//        .reduceGroup(new FilterSrcConsistentClusterVertices2(clipConfig.getSourceNo())).map(new GetF0F1Tuple3());
public class FilterSrcConsistentClusterVertices implements GroupReduceFunction <Tuple2<Vertex, String>, Vertex>{
    private Integer sourceNo;
    public FilterSrcConsistentClusterVertices(Integer inputSourceNo){sourceNo = inputSourceNo;}
    public FilterSrcConsistentClusterVertices(){sourceNo = -1;}

    @Override
    public void reduce(Iterable<Tuple2<Vertex, String>> iterable, Collector<Vertex> collector) throws Exception {
        Collection<String> sources = new ArrayList<>();
        Collection <Vertex> vertices = new ArrayList<>();
        Boolean isSourceConsistent = true;
        for (Tuple2<Vertex, String> vertex:iterable){
            String src = "";
            if (vertex.f0.hasProperty("type"))
                src = vertex.f0.getPropertyValue("type").toString();
            else
                src = vertex.f0.getPropertyValue("graphLabel").toString();

            if (!sources.contains(src))
                sources.add(src);
            else
                isSourceConsistent = false;
            vertices.add(vertex.f0);
        }
        if (isSourceConsistent){
            if ((sourceNo != -1 && vertices.size() == sourceNo) || sourceNo == -1) {
                for (Vertex vertex : vertices)
                    collector.collect(vertex);
            }

        }
    }
}
