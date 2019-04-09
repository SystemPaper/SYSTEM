package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.ExtendedJaccard;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 */
public class ExtendedJaccardComponent extends SimilarityComponent {
    private String tokenizer;
    private Double threshold;
    private Double jaroWinklerThreshold;

    public ExtendedJaccardComponent(
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
            Double Threshold,
            Double JaroWinklerThreshold) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        tokenizer = Tokenizer;
        threshold = Threshold;
        jaroWinklerThreshold = JaroWinklerThreshold;
    }

    public ExtendedJaccardComponent(SimilarityComponentBaseConfig baseConfig, String tokenizer,
      Double threshold, Double jaroWinklerThreshold) {
        super(baseConfig);
        this.tokenizer = tokenizer;
        this.threshold = threshold;
        this.jaroWinklerThreshold = jaroWinklerThreshold;
    }

    public String getTokenizer() {
        return tokenizer;
    }

    public Double getThreshold() {
        return threshold;
    }

    public Double getJaroWinklerThreshold() {
        return jaroWinklerThreshold;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        return new ExtendedJaccard(tokenizer, threshold, jaroWinklerThreshold);
    }
}
