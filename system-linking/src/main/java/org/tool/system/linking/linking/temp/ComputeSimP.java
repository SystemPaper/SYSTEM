package org.tool.system.linking.linking.temp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.tokenizers.Tokenizers;

import java.math.BigDecimal;

import static org.simmetrics.builders.StringMetricBuilder.with;

public class ComputeSimP implements FlatMapFunction<Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, Double>> {
    private Double threshold;

    public ComputeSimP(Double threshold) {
        this.threshold = threshold;
    }

    @Override
    public void flatMap(Tuple2<Vertex, Vertex> input, Collector<Tuple3<Vertex, Vertex, Double>> collector) throws Exception {
        int i = 0;
        double similarity = 0;

        if (input.f0.hasProperty("name")  && input.f1.hasProperty("name")) {

            String name1 = input.f0.getPropertyValue("name").toString().toLowerCase();
            String name2 = input.f1.getPropertyValue("name").toString().toLowerCase();

            if (!(name1.equals("") || name2.equals(""))) {
                i++;
                double sim = getCosineTrigramMetric()
                        .compare(name1, name2);
                similarity += getExactDoubleResult(sim);
            }
        }

        if (input.f0.hasProperty("surname")  && input.f1.hasProperty("surname") ) {

            String surname1 = input.f0.getPropertyValue("surname").toString().toLowerCase();
            String surname2 = input.f1.getPropertyValue("surname").toString().toLowerCase();

            if (!(surname1.equals("") || surname1.equals(""))) {
                i++;

                double sim = getCosineTrigramMetric()
                        .compare(surname1, surname2);
                similarity += getExactDoubleResult(sim);
            }
        }

        if (input.f0.hasProperty("postcod")  && input.f1.hasProperty("postcod") ) {

            String postcod1 = input.f0.getPropertyValue("postcod").toString().toLowerCase();
            String postcod2 = input.f1.getPropertyValue("postcod").toString().toLowerCase();

            if (!(postcod1.equals("") || postcod2.equals(""))) {
                i++;

                double sim = getCosineTrigramMetric()
                        .compare(postcod1, postcod2);
                similarity += getExactDoubleResult(sim);
            }
        }
        if (input.f0.hasProperty("suburb")  && input.f1.hasProperty("suburb") ) {

            String suburb1 = input.f0.getPropertyValue("suburb").toString().toLowerCase();
            String suburb2 = input.f1.getPropertyValue("suburb").toString().toLowerCase();

            if (!(suburb1.equals("") || suburb2.equals(""))) {
                i++;
                double sim = getCosineTrigramMetric()
                        .compare(suburb1, suburb2);
                similarity += getExactDoubleResult(sim);
            }
        }
        if (i != 0)
            similarity /= i;
        similarity = getExactDoubleResult(similarity);
        if (similarity >= threshold)
            collector.collect(Tuple3.of(input.f0, input.f1, similarity));
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
}