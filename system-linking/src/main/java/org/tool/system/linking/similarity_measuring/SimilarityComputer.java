package org.tool.system.linking.similarity_measuring;

import org.apache.flink.hadoop.shaded.com.google.common.base.CharMatcher;
import org.tool.system.linking.similarity_measuring.data_structures.*;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.*;

import java.io.Serializable;
import java.util.List;

public class SimilarityComputer implements Serializable {

    private SimilarityComponent similarityComponent;

    public SimilarityComputer(SimilarityComponent SimilarityComponent) {
        similarityComponent = SimilarityComponent;
    }


    public SimilarityField computeSimilarity(List<String> value1, List<String> value2) throws Exception {

        SimilarityComputation computation = similarityComponent.buildSimilarityComputation();
        double simdegree = computation.computeSimilarity(value1, value2);
        return new SimilarityField(similarityComponent.getId(), simdegree, similarityComponent.getWeight());
    }

    public SimilarityField computeSimilarity(String value1, String value2) throws Exception {
        /*Replacing all non-alphanumeric characters with empty strings*/
//         	  value1 = value1.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().trim();
//            value2 = value2.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().trim();

        /*Replacing 1 or more spaces.*/
//        value1 = value1.toLowerCase().trim().replaceAll("\\s+", " ");
//        value2 = value2.toLowerCase().trim().replaceAll("\\s+", " ");
//        value1 = CharMatcher.WHITESPACE.trimAndCollapseFrom(
//                value1.toLowerCase()
//                        .replaceAll("[\\p{Punct}]", ""),
//                ' ');
//        value2 = CharMatcher.WHITESPACE.trimAndCollapseFrom(
//                value2.toLowerCase()
//                        .replaceAll("[\\p{Punct}]", ""),
//                ' ');

        SimilarityComputation similarityComputation =
          similarityComponent.buildSimilarityComputation();
        double simDegree = similarityComputation.computeSimilarity(value1, value2);
        return new SimilarityField(similarityComponent.getId(), simDegree, similarityComponent.getWeight());
    }

    public double computeGeoDistance(String strlat1, String strlon1, String strlat2, String strlon2) {
        double lat1 = Double.parseDouble(strlat1);
        double lat2 = Double.parseDouble(strlat2);
        double lon1 = Double.parseDouble(strlon1);
        double lon2 = Double.parseDouble(strlon2);
        Double arccosParam = Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon1 - lon2);
        if (arccosParam < -1)
            arccosParam = -1d;
        if (arccosParam > 1)
            arccosParam = 1d;
        double distance = Math.acos(arccosParam) * 6371;
        return distance;
    }

    public double convertDistance2Simdegree(Double distance) {
//        distance /= 6371;
//        if (distance == 0)
//            return 1;
//        distance = 1/(distance*100);
//        if(distance>=1)
//            return 1;
//        return distance;
        if (distance > 13458)
            return 0;
        else return 1;
    }
}
