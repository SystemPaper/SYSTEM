package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the NumericalSimilarity method with a defined maximum percentage of distance
 *
 */
public class NumericalSimilarityWithMaxPercentage implements SimilarityComputation<String> {
    private Double maxToleratedPercentage;

    public NumericalSimilarityWithMaxPercentage(Double MaxToleratedPercentage) {
        maxToleratedPercentage = MaxToleratedPercentage;
    }

    public double computeSimilarity(String value1, String value2){
        return computeSimilarity(Double.parseDouble(value1), Double.parseDouble(value2));
    }

    public double computeSimilarity(Double in1, Double in2) {
        double simdegree;
        Double pc = Math.abs(in1 - in2) * 100 / Math.max(in1, in2);
        if (pc < maxToleratedPercentage)
            simdegree = 1 - (pc / maxToleratedPercentage);
        else simdegree = 0;
        return simdegree;
    }
}
