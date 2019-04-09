package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

import org.tool.system.linking.similarity_measuring.data_structures.SimilarityComputationMethod;

/**
 * computes the similarity degree of two strings using the LongestCommonSubstring method
 */
public class LongestCommonSubstring implements SimilarityComputation<String> {
    private Integer minLength;
    private SimilarityComputationMethod method;

    public LongestCommonSubstring(Integer minLength, SimilarityComputationMethod secondMethod) {
        this.minLength = minLength;
        this.method = secondMethod;
    }

    public double computeSimilarity(String str1, String str2) {
        double simDegree = 0;
        int curLongest = 0;

        int maxLength = Math.max(str1.length(), str2.length());
        int[] v0 = new int[maxLength];
        int[] v1 = new int[maxLength];
        for (int i = 0; i < str1.length(); i++) {
            for (int j = 0; j < str2.length(); j++) {
                if (str1.charAt(i) == str2.charAt(j)) {
                    if (i == 0 || j == 0) {
                        v1[j] = 1;
                    } else {
                        v1[j] = v0[j - 1] + 1;
                    }
                    if (v1[j] > curLongest) {
                        curLongest = v1[j];
                    }
                } else {
                    v1[j] = 0;
                }
            }
            final int[] swap = v0; v0 = v1; v1 = swap;
        }

        switch (method) {
            case OVERLAP:
                simDegree = curLongest / (double) Math.min(str1.length(), str2.length());
                break;
            case JACARD:
                simDegree = curLongest / (double) (str1.length() + str2.length() - curLongest);
                break;
            case DICE:
                simDegree = 2 * curLongest / (double) (str1.length() + str2.length());
                break;
        }
        return simDegree;
    }
}
