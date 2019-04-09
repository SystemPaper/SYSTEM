package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.LongestCommonSubstring;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 */
public class LongestCommSubComponent extends SimilarityComponent {
    private Integer minLength;
    private SimilarityComputationMethod method;

    public LongestCommSubComponent(
            String ComponentId,
            SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Integer MinLength,
            SimilarityComputationMethod SecondMethod) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        minLength = MinLength;
        method = SecondMethod;
    }

    public LongestCommSubComponent(SimilarityComponentBaseConfig baseConfig, Integer minLength,
      SimilarityComputationMethod method) {
        super(baseConfig);
        this.minLength = minLength;
        this.method = method;
    }

    public Integer getMinLength() {
        return minLength;
    }

    public SimilarityComputationMethod getMethod() {
        return method;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        return new LongestCommonSubstring(minLength, method);
    }
}
