package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

/**
 * computes the similarity degree of two strings using the MongeElkanJaroWinkler method
 */
public class MongeElkanJaroWinkler implements SimilarityComputation<String> {
    private String tokenizer;
    private Double threshold;

    public MongeElkanJaroWinkler(String Tokenizer, Double Threshold) {
        tokenizer = Tokenizer;
        threshold = Threshold;
    }

    public double computeSimilarity(String str1, String str2) {
        double simdegree = 0;
        String[] str1tokens = str1.split(tokenizer);
        String[] str2tokens = str2.split(tokenizer);
        double max = 0;
        JaroWinkler jw = new JaroWinkler(threshold);
        for (String s1 : str1tokens) {
            for (String s2 : str2tokens) {
                double jwsim = jw.computeSimilarity(s1, s2);
                if (jwsim > max) max = jwsim;
            }
            simdegree += max;
        }
        simdegree /= str1tokens.length;
        return simdegree;
    }
}
