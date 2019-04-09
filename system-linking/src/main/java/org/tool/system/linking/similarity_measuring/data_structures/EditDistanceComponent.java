package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.EditDistanceLevenshtein;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

public class EditDistanceComponent extends SimilarityComponent {

  public EditDistanceComponent(String componentId,
    SimilarityComputationMethod similarityComputationMethod, String sourceGraphLabel,
    String srcLabel, String srcAttribute, String targetGraphLabel, String targetLabel,
    String targetAttribute, Double weight) {
    super(componentId, similarityComputationMethod, sourceGraphLabel, srcLabel, srcAttribute,
      targetGraphLabel, targetLabel, targetAttribute, weight);
  }

  public EditDistanceComponent(SimilarityComponentBaseConfig baseConfig){
    super(baseConfig);
  }

  @Override
  public SimilarityComputation buildSimilarityComputation() {
    return new EditDistanceLevenshtein();
  }
}
