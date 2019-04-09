package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;


public class ListSimilarity implements SimilarityComputation<List<String>> {

  private SimilarityComputation elementSimilarityComputation;
  private AggregationMethod aggregationMethod;


  public ListSimilarity(SimilarityComputation elementSimilarityComputation,
    AggregationMethod aggregationMethod){
    this.elementSimilarityComputation = elementSimilarityComputation;
    this.aggregationMethod = aggregationMethod;



  }
  @SuppressWarnings("unchecked")
  public double computeSimilarity(List<String> list1, List<String> list2){
    List<Double> elementSimilarities = new ArrayList<>();
    for(String list1Element : list1 ) {
      for(String list2Element: list2){
        list1Element = list1Element.toLowerCase().trim().replaceAll("\\s+", " ");
        list2Element = list2Element.toLowerCase().trim().replaceAll("\\s+", " ");
        elementSimilarities.add(elementSimilarityComputation.computeSimilarity(list1Element,
          list2Element));
      }
    }

    return aggregationMethod.apply(elementSimilarities);
  }


  public interface AggregationMethod extends Function<List<Double>, Double> {

    static AggregationMethod fromString(String name) {
      switch (name) {
      case "AggregationMin":
        return new AggregationMethodMin();
      case "AggregationMax":
        return new AggregationMethodMax();
      case "AggregationAv":
        return new AggregationMethodAverage();
      default:
        throw new IllegalArgumentException("AggregationMethod must be one of " +
          "Aggregation{Min|Max|Av}");
      }
    }
  }

  public static class AggregationMethodMin implements AggregationMethod {
    @Override
    public Double apply(List<Double> similarities) {
      return similarities.stream().min(Double::compareTo).orElse(0d);
    }
  }

  public static class AggregationMethodMax implements AggregationMethod {
    @Override
    public Double apply(List<Double> similarities) {
      return similarities.stream().max(Double::compareTo).orElse(0d);
    }
  }

  public static class AggregationMethodAverage implements AggregationMethod {
    @Override
    public Double apply(List<Double> similarities) {
      return similarities.stream().mapToDouble(d -> d).average().orElse(0d);
    }
  }

}
