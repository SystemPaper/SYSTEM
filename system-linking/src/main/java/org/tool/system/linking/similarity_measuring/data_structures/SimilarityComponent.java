package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

import java.io.Serializable;

/**
 */
public abstract class SimilarityComponent implements Serializable {
    private String srcAttribute;
    private String targetAttribute;
    private String srcGraphLabel;
    private String targetGraphLabel;
    private String srcLabel;
    private String targetLabel;
    private Double weight;
    protected SimilarityComputationMethod similarityComputationMethod;
    private String id;

    public SimilarityComponent(
      String componentId,
      SimilarityComputationMethod similarityComputationMethod,
      String sourceGraphLabel,
      String srcLabel,
      String srcAttribute,
      String targetGraphLabel,
      String targetLabel,
      String targetAttribute,
      Double weight) {
        this.srcAttribute = srcAttribute;
        this.targetAttribute = targetAttribute;
        this.srcGraphLabel = sourceGraphLabel;
        this.targetGraphLabel = targetGraphLabel;
        this.srcLabel = srcLabel;
        this.targetLabel = targetLabel;
        this.weight = weight;
        this.similarityComputationMethod = similarityComputationMethod;
        this.id = componentId;
    }

    public SimilarityComponent(SimilarityComponentBaseConfig config) {
        this.srcAttribute = config.srcAttribute;
        this.targetAttribute = config.targetAttribute;
        this.srcGraphLabel = config.srcGraphLabel;
        this.targetGraphLabel = config.targetGraphLabel;
        this.srcLabel = config.srcLabel;
        this.targetLabel = config.targetLabel;
        this.weight = config.weight;
        this.similarityComputationMethod = config.similarityComputationMethod;
        this.id = config.id;
    }

    public SimilarityComputationMethod getSimilarityComputationMethod() {
        return similarityComputationMethod;
    }

    public abstract SimilarityComputation buildSimilarityComputation();

    public String getSrcAttribute() {
        return srcAttribute;
    }

    public String getTargetAttribute() {
        return targetAttribute;
    }

    public String getId() {
        return id;
    }

    public Double getWeight() {
        return weight;
    }

    public String getSrcGraphLabel() {
        return srcGraphLabel;
    }

    public void setSrcGraphLabel(String srcGraphLabel) {
        this.srcGraphLabel = srcGraphLabel;
    }

    public String getTargetGraphLabel() {
        return targetGraphLabel;
    }

    public void setTargetGraphLabel(String targetGraphLabel) {
        this.targetGraphLabel = targetGraphLabel;
    }

    public String getSrcLabel() {
        return srcLabel;
    }

    public String getTargetLabel() {
        return targetLabel;
    }


    // TODO this is a workaround for the existing unit-tests
    public static class SimilarityComponentBaseConfig{
        String srcAttribute;
        String targetAttribute;
        String srcGraphLabel;
        String targetGraphLabel;
        String srcLabel;
        String targetLabel;
        Double weight;
        SimilarityComputationMethod similarityComputationMethod;
        String id;
        public SimilarityComponentBaseConfig(
          String componentId,
          SimilarityComputationMethod similarityComputationMethod,
          String sourceGraphLabel,
          String srcLabel,
          String srcAttribute,
          String targetGraphLabel,
          String targetLabel,
          String targetAttribute,
          Double weight) {
            this.srcAttribute = srcAttribute;
            this.targetAttribute = targetAttribute;
            this.srcGraphLabel = sourceGraphLabel;
            this.targetGraphLabel = targetGraphLabel;
            this.srcLabel = srcLabel;
            this.targetLabel = targetLabel;
            this.weight = weight;
            this.similarityComputationMethod = similarityComputationMethod;
            this.id = componentId;
        }
    }

}
