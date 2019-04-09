package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.NumericalSimilarityWithMaxDistance;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 */
public class NumericalSimilarityWithMaxDistanceComponent extends SimilarityComponent {
    private Double maxToleratedDis;

    public NumericalSimilarityWithMaxDistanceComponent(
            String ComponentId,
            SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Double MaxToleratedDis) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        maxToleratedDis = MaxToleratedDis;
    }

    public NumericalSimilarityWithMaxDistanceComponent(SimilarityComponentBaseConfig baseConfig,
      Double maxToleratedDis) {
        super(baseConfig);
        this.maxToleratedDis = maxToleratedDis;
    }

    public Double getMaxToleratedDis() {
        return maxToleratedDis;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        return new NumericalSimilarityWithMaxDistance(maxToleratedDis);
    }
}
