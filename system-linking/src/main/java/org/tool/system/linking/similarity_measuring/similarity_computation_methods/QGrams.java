package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

import org.tool.system.linking.similarity_measuring.data_structures.SimilarityComputationMethod;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.tokenizers.Tokenizers;

import static org.simmetrics.builders.StringMetricBuilder.with;

/**
 * computes the similarity degree of two strings using the QGram method
 */
public class QGrams implements SimilarityComputation<String> {

    private Integer length;
    private Boolean padding;
    private SimilarityComputationMethod method;

    public QGrams(Integer Length, Boolean Padding, SimilarityComputationMethod SecondMethod) {
        length = Length;
        padding = Padding;
        method = SecondMethod;
    }

    public double computeSimilarity(String str1, String str2) {
//        if ((str1.equals("0=0 - The GoalThe Satisfaction of Giving Up")
//                && str2.contains("0=0The Goal (The Satisfaction of Giving Up)The Satisfaction of Giving Up"))
//                ||
//                (str2.equals("0=0 - The GoalThe Satisfaction of Giving Up")
//                        && str1.contains("0=0The Goal (The Satisfaction of Giving Up)The Satisfaction of Giving Up")))
//            System.out.println(" *************************");

//        str1 = CharMatcher.WHITESPACE.trimAndCollapseFrom(
//                str1.toLowerCase()
//                        .replaceAll("[\\p{Punct}]", " "),
//                ' ');
//
//        str2 = CharMatcher.WHITESPACE.trimAndCollapseFrom(
//                str2.toLowerCase()
//                        .replaceAll("[\\p{Punct}]", " "),
//                ' ');
        if (str1.equals("") || str2.equals(""))
            return -1;

        double similarity = getCosineTrigramMetric(length)
                .compare(str1, str2);
        return similarity;


//        return new BigDecimal(similarity)
//                .setScale(6, BigDecimal.ROUND_HALF_UP)
//                .doubleValue();

        /*
        if (str1.length() < length || str2.length() < length) {
            return str1.equals(str2) ? 1d : 0d;
        }

        double simdegree = 0d;
        Collection<String> str1QGrams = new ArrayList<String>();
        Collection<String> str2QGrams = new ArrayList<String>();
        if (padding) {
            if (str1.length() >= length) {
                str1QGrams.add(str1.substring(0, length - 1));
                str1QGrams.add(str1.substring(str1.length() - length + 1));
            }
            if (str2.length() >= length) {
                str2QGrams.add(str2.substring(0, length - 1));
                str2QGrams.add(str2.substring(str2.length() - length + 1));
            }
        }
        for (int i = 0; i < str1.length() && i + length <= str1.length(); i++)
            str1QGrams.add(str1.substring(i, i + length));
        for (int i = 0; i < str2.length() && i + length <= str2.length(); i++)
            str2QGrams.add(str2.substring(i, i + length));

        int str1QGramsSize = str1QGrams.size();
        int str2QGramsSize = str2QGrams.size();

        for (String s : str1QGrams) {
            if (str2QGrams.contains(s)) {
                simdegree++;
                str2QGrams.remove(s);
            }
        }
        switch (method) {
            case OVERLAP:
                simdegree = simdegree / Math.min(str1QGramsSize, str2QGramsSize);
                break;
            case JACARD:
                simdegree = simdegree / (str1QGramsSize + str2QGramsSize - simdegree);
                break;
            case DICE:
                simdegree = (float) 2 * simdegree / (str1QGramsSize + str2QGramsSize);
                break;
        }
//        if (simdegree>=0.35)
//        System.out.println(method+"---"+str1+"---"+str2+"---"+simdegree);

        return simdegree;
        */
    }
    private static StringMetric getCosineTrigramMetric(int length) {
        return with(new CosineSimilarity<>())
                .tokenize(Tokenizers.qGramWithPadding(length))
                .build();
        // old:
        // .simplify(Simplifiers.removeAll("[\\\\(|,].*"))
        // .simplify(Simplifiers.replaceNonWord())
    }
}

