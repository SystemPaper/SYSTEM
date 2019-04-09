package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.TruncateBegin;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.TruncateEnd;

/**
 */
public class TruncateComponent extends SimilarityComponent {
    private Integer length;

    public TruncateComponent(
            String ComponentId,
            SimilarityComputationMethod SimilarityComputationMethod,
            String SourceGraphId,
            String sourceLabel,
            String SrcAttribute,
            String TargetGraphId,
            String targetLabel,
            String TargetAttribute,
            Double Weight,
            Integer Length) {
        super(ComponentId, SimilarityComputationMethod, SourceGraphId, sourceLabel, SrcAttribute, TargetGraphId, targetLabel, TargetAttribute, Weight);
        length = Length;
    }

    public TruncateComponent(SimilarityComponentBaseConfig baseConfig, Integer length) {
        super(baseConfig);
        this.length = length;
    }

    public Integer getLength() {
        return length;
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {
        if (SimilarityComputationMethod.TRUNCATE_BEGIN.equals(similarityComputationMethod)){
            return new TruncateBegin(length);
        } else if (SimilarityComputationMethod.TRUNCATE_END.equals(similarityComputationMethod)) {
            return new TruncateEnd(length);
        } else {
            return null;
        }
    }
}
