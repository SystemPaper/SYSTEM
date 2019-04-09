package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddClsIdNewPropRmvVP implements MapFunction <Vertex, Tuple2<Vertex, Boolean>> {
    private String prefix;
    public AddClsIdNewPropRmvVP(String prefix) {this.prefix = prefix;}
    @Override
    public Tuple2<Vertex, Boolean> map(Vertex vertex) throws Exception {
        boolean isNew = false;
        if (!vertex.hasProperty("ClusterId")){
            vertex.setProperty("ClusterId", vertex.getPropertyValue("VertexPriority")+"-"+prefix);
            if (!vertex.hasProperty("new"))
                vertex.setProperty("new", "");
            isNew = true;
        }
        vertex.removeProperty("VertexPriority");
        return Tuple2.of(vertex, isNew);
    }
}
