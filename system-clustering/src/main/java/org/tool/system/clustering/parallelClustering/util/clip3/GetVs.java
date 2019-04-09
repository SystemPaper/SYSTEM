package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;

import javax.tools.JavaCompiler;

public class GetVs implements JoinFunction <Tuple3<String, String, String>, Tuple2<Vertex, String>, Vertex> {

    @Override
    public Vertex join(Tuple3<String, String, String> in1, Tuple2<Vertex, String> in2) throws Exception {
        in2.f0.setProperty("ClusterId", in1.f2);
        return in2.f0;
    }
}
