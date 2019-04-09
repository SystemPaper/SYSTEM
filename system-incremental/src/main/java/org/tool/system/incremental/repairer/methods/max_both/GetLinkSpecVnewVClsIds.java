package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetLinkSpecVnewVClsIds implements MapFunction <Tuple3<Edge, Vertex, Vertex>,
        Tuple6<String, Double, Vertex, Vertex, String, String>> {
    @Override
    public Tuple6<String, Double, Vertex, Vertex, String, String> map(Tuple3<Edge, Vertex, Vertex> in) throws Exception {
        if (in.f1.hasProperty("new"))
            return Tuple6.of(in.f0.getId().toString(), Double.parseDouble(in.f0.getPropertyValue("value").toString()),
                    in.f2, in.f1, in.f2.getPropertyValue("ClusterId").toString(), in.f1.getPropertyValue("ClusterId").toString());
        else
            return Tuple6.of(in.f0.getId().toString(), Double.parseDouble(in.f0.getPropertyValue("value").toString()),
                    in.f1, in.f2, in.f1.getPropertyValue("ClusterId").toString(), in.f2.getPropertyValue("ClusterId").toString());
    }
}
