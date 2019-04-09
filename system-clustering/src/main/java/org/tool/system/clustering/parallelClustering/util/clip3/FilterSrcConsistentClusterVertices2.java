package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class FilterSrcConsistentClusterVertices2 implements GroupReduceFunction <Tuple3<String, String, String>, Tuple3<String, String, String>>{
    private Integer sourceNo;
    public FilterSrcConsistentClusterVertices2(Integer inputSourceNo){sourceNo = inputSourceNo;}
    public FilterSrcConsistentClusterVertices2(){sourceNo = -1;}

    @Override
    public void reduce(Iterable<Tuple3<String, String, String>> iterable, Collector<Tuple3<String, String, String>> collector) throws Exception {
        Collection<String> sources = new ArrayList<>();
        Collection <Tuple3<String, String, String>> vertices = new ArrayList<>();
        Boolean isSourceConsistent = true;
        for (Tuple3<String, String, String> vertex:iterable){
            String src = vertex.f1;

            if (!sources.contains(src))
                sources.add(src);
            else
                isSourceConsistent = false;
            vertices.add(vertex);
        }
        if (isSourceConsistent){
            if ((sourceNo != -1 && vertices.size() == sourceNo) || sourceNo == -1) {
                for (Tuple3<String, String, String> vertex : vertices)
                    collector.collect(vertex);
            }

        }
    }
}
