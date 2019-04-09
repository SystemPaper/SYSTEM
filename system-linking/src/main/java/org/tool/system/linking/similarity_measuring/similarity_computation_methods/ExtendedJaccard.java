package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

/**
 * * computes the similarity degree of two strings using the ExtendedJaccard method
 */
public class ExtendedJaccard implements SimilarityComputation<String> {
    private String tokenizer;
    private Double threshold;
    private Double jaroWinklerThreshold;

    public ExtendedJaccard(String Tokenizer, Double Threshold, Double JaroWinklerThreshold) {
        tokenizer = Tokenizer;
        threshold = Threshold;
        jaroWinklerThreshold = JaroWinklerThreshold;
    }
    public double computeSimilarity(String str1, String str2) {
        double simdegree;
        String[] str1tokens = str1.split(tokenizer);
        String[] str2tokens = str2.split(tokenizer);
        JaroWinkler jw = new JaroWinkler(jaroWinklerThreshold);

        Boolean[] similarStr1tokens = new Boolean[str1tokens.length];
        Boolean[] similarStr2tokens = new Boolean[str2tokens.length];

        for (int i = 0; i < similarStr1tokens.length; i++)
            similarStr1tokens[i] = false;

        for (int i = 0; i < similarStr2tokens.length; i++)
            similarStr2tokens[i] = false;

        for (int i = 0; i < str1tokens.length; i++){
            for (int j = 0; j < str2tokens.length; j++){
                double jwsim = jw.computeSimilarity(str1tokens[i],str2tokens[j]);
                if (jwsim >= threshold) {
                    similarStr1tokens[i] = true;
                    similarStr2tokens[j] = true;
                }
            }
        }
        int similarStr1tokenNo = 0;
        int similarStr2tokenNo = 0;

        for (int i = 0; i < similarStr1tokens.length; i++)
            if (similarStr1tokens[i])
                similarStr1tokenNo++;

        for (int i = 0; i < similarStr2tokens.length; i++)
            if (similarStr2tokens[i])
                similarStr2tokenNo++;

        int similarTokensSet = Math.max(similarStr1tokenNo, similarStr2tokenNo);

        simdegree = (double)similarTokensSet/ (similarStr1tokens.length-similarStr1tokenNo+similarStr2tokens.length-similarStr2tokenNo+similarTokensSet);
        return simdegree;
    }
}
