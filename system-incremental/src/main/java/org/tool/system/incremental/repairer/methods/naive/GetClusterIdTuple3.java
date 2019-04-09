package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Vertex;
//V_old_null
public class GetClusterIdTuple3 implements MapFunction <Vertex, Tuple3<Vertex,String, String>> {
    @Override
    public Tuple3<Vertex, String, String> map(Vertex vertex) throws Exception {
        return Tuple3.of(vertex, vertex.getPropertyValue("ClusterId").toString(), "");
    }
}
