package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
//Vid_src
public class Vertex2IdSrc implements MapFunction <Vertex, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> map(Vertex vertex) throws Exception {
        return Tuple2.of(vertex.getId().toString(), vertex.getPropertyValue("graphLabel").toString());
    }
}
