package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.NumericalSimilarityWithMaxPercentage;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 */
public class NumericalSimilarityWithMaxPercentageComponent extends SimilarityComponent {
    private Double maxToleratedPercentage;

    public NumericalSimilarityWithMaxPercentageComponent(
            String ComponentId,
            SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Double MaxToleratedPercentage) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        maxToleratedPercentage = MaxToleratedPercentage;
    }

    public NumericalSimilarityWithMaxPercentageComponent(SimilarityComponentBaseConfig baseConfig
      , Double maxToleratedPercentage) {
        super(baseConfig);
        this.maxToleratedPercentage = maxToleratedPercentage;
    }

    public Double getMaxToleratedPercentage() {
        return maxToleratedPercentage;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        return new NumericalSimilarityWithMaxPercentage(maxToleratedPercentage);
    }
}
