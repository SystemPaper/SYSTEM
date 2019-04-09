package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;

public class Vertex2ClsIdSrc implements MapFunction <Vertex, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> map(Vertex vertex) throws Exception {
        String src = "";
        if (vertex.hasProperty("graphLabel"))
            src = vertex.getPropertyValue("graphLabel").toString();
        else if (vertex.hasProperty("type"))
            src = vertex.getPropertyValue("type").toString();
        else{
            List<PropertyValue> vertices_graphLabel = vertex.getPropertyValue("vertices_graphLabel").getList();
            for(Object label: vertices_graphLabel){
                src += (","+label);
            }
            src = src.substring(1);
        }

        return Tuple2.of(vertex.getPropertyValue("ClusterId").toString(), src);
    }
}
