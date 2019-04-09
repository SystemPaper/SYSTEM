package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.MongeElkanJaroWinkler;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 */
public class MongeElkanComponent extends SimilarityComponent {
    private String tokenizer;
    private Double threshold;

    public MongeElkanComponent(
            String ComponentId,
            SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            String Tokenizer,
            Double Threshold) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        threshold = Threshold;
        tokenizer = Tokenizer;
    }

    public MongeElkanComponent(SimilarityComponentBaseConfig baseConfig, String tokenizer,
      Double threshold) {
        super(baseConfig);
        this.tokenizer = tokenizer;
        this.threshold = threshold;
    }

    public String getTokenizer() {
        return tokenizer;
    }

    public Double getThreshold() {
        return threshold;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        return new MongeElkanJaroWinkler(tokenizer, threshold);
    }
}
