package org.tool.system.linking.similarity_measuring.preparation.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.tool.system.linking.similarity_measuring.data_structures.EmbeddingComponent;

/**
 * Collect and processing words that are written to temporary corpus
 */
public class CollectPropertiesForCorpus implements FlatMapFunction<Vertex, String> {

    private final EmbeddingComponent component;

    public CollectPropertiesForCorpus(EmbeddingComponent component) {
        this.component = component;
    }

    @Override
    public void flatMap(Vertex vertex, Collector<String> collector) {
        String graphLabel = vertex.getPropertyValue("graphLabel").getString();
        if (component.getSrcGraphLabel().equals(graphLabel) || component.getSrcLabel().equals("*")) {
            if (component.getSrcLabel().equals(vertex.getLabel())) {
                PropertyValue propVal = vertex.getPropertyValue(component.getSrcAttribute());
                if (propVal != null) collector.collect(propVal.getString().toLowerCase());
            }
        } else if (component.getTargetGraphLabel().equals(graphLabel) || component.getTargetGraphLabel().equals("*")) {
            if (component.getTargetLabel().equals(vertex.getLabel())) {
                PropertyValue propVal = vertex.getPropertyValue(component.getTargetAttribute());
                if (propVal != null) collector.collect(propVal.getString().toLowerCase());
            }
        }
    }
}
