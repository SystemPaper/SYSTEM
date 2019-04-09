package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.QGrams;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 */
public class QGramsComponent extends SimilarityComponent {
    private Integer length;
    private Boolean padding;
    private SimilarityComputationMethod method;

    public QGramsComponent(
            String ComponentId,
            SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Integer Length,
            Boolean Padding,
            SimilarityComputationMethod SecondMethod) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        length = Length;
        padding = Padding;
        method = SecondMethod;
    }

    public QGramsComponent(SimilarityComponentBaseConfig baseConfig, Integer length,
      Boolean padding, SimilarityComputationMethod method) {
        super(baseConfig);
        this.length = length;
        this.padding = padding;
        this.method = method;
    }

    public Integer getLength() {
        return length;
    }

    public Boolean getPadding() {
        return padding;
    }

    public SimilarityComputationMethod getMethod() {
        return method;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        return new QGrams(length, padding, method);
    }
}
