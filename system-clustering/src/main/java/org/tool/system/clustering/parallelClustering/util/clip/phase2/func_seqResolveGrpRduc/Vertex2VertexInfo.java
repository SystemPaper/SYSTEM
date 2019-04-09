package org.tool.system.clustering.parallelClustering.util.clip.phase2.func_seqResolveGrpRduc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class Vertex2VertexInfo implements MapFunction <Vertex, Tuple3<String, String, String>>{
    @Override
    public Tuple3<String, String, String> map(Vertex vertex) throws Exception {
        String src = "";
        if (vertex.hasProperty("type"))
            src = vertex.getPropertyValue("type").toString();
        else
            src = vertex.getPropertyValue("graphLabel").toString();

        return Tuple3.of(vertex.getId().toString(), src,
                vertex.getPropertyValue("ClusterId").toString());
    }
}
