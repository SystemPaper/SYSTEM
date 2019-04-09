package org.tool.system.linking.linking.temp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.hadoop.shaded.com.google.common.base.CharMatcher;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.omg.CORBA.Object;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.tokenizers.Tokenizers;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.simmetrics.builders.StringMetricBuilder.with;

public class ComputeSimM implements FlatMapFunction<Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, Double>> {
    private Double threshold;
    private Boolean isRepetitionAllowed;

    public ComputeSimM(Double threshold,  Boolean isRepetitionAllowed) {

        this.threshold = threshold;
        this.isRepetitionAllowed = isRepetitionAllowed;
    }

    @Override
    public void flatMap(Tuple2<Vertex, Vertex> input, Collector<Tuple3<Vertex, Vertex, Double>> collector) throws Exception {
        ArrayList field1 = new ArrayList();
        ArrayList field2 = new ArrayList();
        ArrayList<Double> simList = new ArrayList<>();



        if (input.f0.hasProperty("field"))
            field1.add(input.f0.getPropertyValue("field").toString().toLowerCase());
        else  if (input.f0.hasProperty("vertices_field")) {
            field1.addAll(input.f0.getPropertyValue("vertices_field").getList());
            if (isRepetitionAllowed)
                field1 = clean (field1);
        }

        if (input.f1.hasProperty("field"))
            field2.add(input.f1.getPropertyValue("field").toString().toLowerCase());
        else if (input.f1.hasProperty("vertices_field")) {
            field2.addAll(input.f1.getPropertyValue("vertices_field").getList());
            if (isRepetitionAllowed)
                field2 = clean (field2);
        }




        for (java.lang.Object f1:field1){
            String f1Str = f1.toString();
//            f1Str =  CharMatcher.WHITESPACE.trimAndCollapseFrom(
//                    f1Str.toLowerCase()
//                            .replaceAll("[\\p{Punct}]", " "),
//                    ' ');

            for (java.lang.Object f2:field2){
                String f2Str = f2.toString();
//                f2Str =  CharMatcher.WHITESPACE.trimAndCollapseFrom(
//                        f2Str.toLowerCase()
//                                .replaceAll("[\\p{Punct}]", " "),
//                        ' ');
                if (!(f1Str.equals("") || f2Str.equals(""))) {
                    double sim = getCosineTrigramMetric()
                            .compare(f1Str, f2Str);
                    if (getExactDoubleResult(sim) >= threshold)
                        simList.add(sim);

                }
            }
        }




    }
    private StringMetric getCosineTrigramMetric() {
        return with(new CosineSimilarity<>())
                .tokenize(Tokenizers.qGramWithPadding(3))
                .build();
    }

    private double getExactDoubleResult(double value) {
        return new BigDecimal(value)
                .setScale(6, BigDecimal.ROUND_HALF_UP)
                .doubleValue();
    }
    private ArrayList clean(ArrayList input){
        ArrayList output = new ArrayList();
        for (java.lang.Object in: input){
            if (!output.contains(in))
                output.add(in);
        }
        return output;
    }
}