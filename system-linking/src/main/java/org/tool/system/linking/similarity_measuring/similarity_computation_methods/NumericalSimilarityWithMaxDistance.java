package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the NumericalSimilarity method with a defined maximum distance
 * TODO String Generic is just a workaround not to break the existing code
 */
public class NumericalSimilarityWithMaxDistance implements SimilarityComputation<String> {
    private Double maxToleratedDis;

    public NumericalSimilarityWithMaxDistance(Double MaxToleratedDis) {
        maxToleratedDis = MaxToleratedDis;
    }

    public double computeSimilarity(String value1, String value2) {
        return computeSimilarity(Double.parseDouble(value1), Double.parseDouble(value2));
    }
    public double computeSimilarity(Double in1, Double in2) {
        double simdegree;
        if (Math.abs(in1 - in2) < maxToleratedDis)
            simdegree = 1 - (Math.abs(in1 - in2) / maxToleratedDis);
        else simdegree = 0;
        return simdegree;
    }
}
