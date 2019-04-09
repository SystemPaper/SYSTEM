package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

/**
 * Computes the similarity of two Objects of Type T
 */
public interface SimilarityComputation<T> {
  double computeSimilarity(T value1, T value2);
}
