package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.JaroWinkler;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 */
public class JaroWinklerComponent extends SimilarityComponent {
    private Double threshold;

    public JaroWinklerComponent(
            String ComponentId,
            SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Double Threshold) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        threshold = Threshold;
    }

    public JaroWinklerComponent(SimilarityComponentBaseConfig baseConfig, Double threshold) {
        super(baseConfig);
        this.threshold = threshold;
    }

    public Double getThreshold() {
        return threshold;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        return new JaroWinkler(threshold);
    }
}
