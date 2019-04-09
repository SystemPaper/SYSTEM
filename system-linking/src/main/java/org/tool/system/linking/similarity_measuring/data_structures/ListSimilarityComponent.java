package org.tool.system.linking.similarity_measuring.data_structures;


import org.tool.system.linking.similarity_measuring.similarity_computation_methods.ListSimilarity;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

/**
 * Computes the similarity of a list property.
 * To achieve this a further SimilarityComponent and an a aggregation strategy is needed. The
 * list elements of the source and target object are compared element wise with the given
 * SimilarityComponent. The resulting similarities are aggregated to a single similarity with the
 * given aggregation strategy.
 *
 * To configure this component in a similiarity config (json) a field "aggregationStrategy" with
 * the aggregation strategy and a field "elementSimilarityComponent" containing a whole
 * similarity component is needed.
 */
public class ListSimilarityComponent extends SimilarityComponent {

  private SimilarityComponent elementSimilarityComponent;
  private ListSimilarity.AggregationMethod aggregationMethod;

  public ListSimilarityComponent(String componentId,
    SimilarityComputationMethod similarityComputationMethod, String sourceGraphLabel,
    String srcLabel, String srcAttribute, String targetGraphLabel, String targetLabel,
    String targetAttribute, Double weight, SimilarityComponent elementSimilarityComponent,
    ListSimilarity.AggregationMethod aggregationMethod) {
    super(componentId, similarityComputationMethod, sourceGraphLabel, srcLabel, srcAttribute,
      targetGraphLabel, targetLabel, targetAttribute, weight);

    this.elementSimilarityComponent = elementSimilarityComponent;
    this.aggregationMethod = aggregationMethod;
  }

  public SimilarityComponent getElementSimilarityComponent(){
    return elementSimilarityComponent;
  }

  public ListSimilarity.AggregationMethod getAggregationMethod(){
    return aggregationMethod;
  }

  @Override
  public SimilarityComputation buildSimilarityComputation() {
      return new ListSimilarity(elementSimilarityComponent.buildSimilarityComputation(),
      aggregationMethod);
  }

}
