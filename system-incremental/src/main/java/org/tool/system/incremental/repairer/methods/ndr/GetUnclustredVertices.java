package org.tool.system.incremental.repairer.methods.ndr;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class GetUnclustredVertices implements FlatMapFunction<Vertex, Vertex> {
    @Override
    public void flatMap(Vertex vertex, Collector<Vertex> output) throws Exception {
        if (!vertex.hasProperty("ClusterId"))
            output.collect(vertex);

    }
}
