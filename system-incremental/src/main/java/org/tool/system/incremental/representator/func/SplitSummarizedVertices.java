package org.tool.system.incremental.representator.func;

//DataSet<Tuple3<Vertex, String, String>> vertex_newId_oldId = input.getVertices().flatMap(new SplitSummarizedVertices());

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Iterator;
import java.util.List;

public class SplitSummarizedVertices implements FlatMapFunction <Vertex, Tuple4<Vertex, String, String, String>>{
    private VertexFactory vertexFactory;
    private String keyProperty;
    private String groupKeyProperty;
    public SplitSummarizedVertices(VertexFactory vertexFactory, String keyProperty){
        this.vertexFactory = vertexFactory;
        this.keyProperty = keyProperty;
        this.groupKeyProperty = "vertices_"+keyProperty;

    }
    @Override
    public void flatMap(Vertex vertex, Collector<Tuple4<Vertex, String, String, String>> collector) throws Exception {
        boolean isSummarized = false;
        if (vertex.hasProperty(this.groupKeyProperty))
            isSummarized = true;
        if (!isSummarized)
            collector.collect(Tuple4.of(vertex, vertex.getId().toString(), vertex.getId().toString(), vertex.getPropertyValue(keyProperty).toString()));
        else {
            List<PropertyValue> vertices_graphLabel = vertex.getPropertyValue(this.groupKeyProperty).getList();
            Vertex[] vertices = new Vertex[vertices_graphLabel.size()];
            for (int i=0; i< vertices.length; i++){
                vertices[i] = this.vertexFactory.createVertex();
            }
            Iterator<String> iterator = vertex.getPropertyKeys().iterator();
            while (iterator.hasNext()){
                String key = iterator.next();
                if (!key.equals("ClusterId")) {
                    List<PropertyValue> PVList = vertex.getPropertyValue(key).getList();
                    int i = 0;
                    key = key.replace("vertices_", "");
                    for (PropertyValue value : PVList) {
                        vertices[i].setProperty(key, value);
                        i++;
                    }
                }
            }
            for (int i=0; i< vertices.length; i++){
                collector.collect(Tuple4.of(vertices[i], vertices[i].getId().toString(),
                        vertex.getId().toString(), vertices[i].getPropertyValue(keyProperty).toString()));
            }
        }
    }
}
