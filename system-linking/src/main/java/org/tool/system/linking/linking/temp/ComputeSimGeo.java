package org.tool.system.linking.linking.temp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.tokenizers.Tokenizers;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.JaroWinkler;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;

import static org.simmetrics.builders.StringMetricBuilder.with;

public class ComputeSimGeo implements FlatMapFunction<Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, Double>> {
    private Double threshold;
    private Boolean isRepetitionAllowed;

    public ComputeSimGeo(Double threshold, Boolean isRepetitionAllowed) {

        this.threshold = threshold;
        this.isRepetitionAllowed = isRepetitionAllowed;
    }

    @Override
    public void flatMap(Tuple2<Vertex, Vertex> input, Collector<Tuple3<Vertex, Vertex, Double>> collector) throws Exception {
        String label1 = "";
        ArrayList labelList1 = new ArrayList();
        String label2 = "";
        ArrayList labelList2 = new ArrayList();

        String lat_lon1 = "";
        ArrayList lat_lonList1 = new ArrayList();
        String lat_lon2 = "";
        ArrayList lat_lonList2 = new ArrayList();







        if (input.f0.hasProperty("label")) {
            label1 = input.f0.getPropertyValue("label").toString();
            if (input.f0.hasProperty("lat_lon")) {
                lat_lon1 = input.f0.getPropertyValue("lat_lon").toString();
            }
        }
        else if (input.f0.hasProperty("vertices_label")) {
            labelList1.addAll(input.f0.getPropertyValue("vertices_label").getList());
            if(input.f0.hasProperty("vertices_lat_lon"))
                lat_lonList1.addAll(input.f0.getPropertyValue("vertices_lat_lon").getList());
            if (isRepetitionAllowed) {
                labelList1 = clean(labelList1);
                lat_lonList1 = clean(lat_lonList1);
            }
        }

        if (input.f1.hasProperty("label")) {
            label2 = input.f1.getPropertyValue("label").toString().toLowerCase();
            if (input.f1.hasProperty("lat_lon")) {
                lat_lon2 = input.f1.getPropertyValue("lat_lon").toString();
            }
        }
        else if (input.f1.hasProperty("vertices_label")) {
            labelList2.addAll(input.f1.getPropertyValue("vertices_label").getList());
            if(input.f1.hasProperty("vertices_lat_lon"))
                lat_lonList2.addAll(input.f1.getPropertyValue("vertices_lat_lon").getList());
            if (isRepetitionAllowed) {
                labelList2 = clean(labelList2);
                lat_lonList2 = clean(lat_lonList2);
            }
        }


        boolean isComputeSim = false;
        if (!label1.equals("") && !label2.equals("")){ //pair e_e
            if (!lat_lon1.equals("") && !lat_lon2.equals("")){
                    if (computeGeoDistance(lat_lon1, lat_lon2))
                        isComputeSim = true;
            }
            else
                isComputeSim = true;
            if (isComputeSim) {
                double sim = new JaroWinkler(0.7).computeSimilarity(label1, label2);
                if (sim >= threshold)
                    collector.collect(Tuple3.of(input.f0, input.f1, sim));
            }
        }
        else {    // pair rep_e
            if (label1.equals("")) {
                label1 = label2;
                lat_lon1 = lat_lon2;
            }
            else {
                labelList1.addAll(labelList2);
                lat_lonList1.addAll(lat_lonList2);
            }
            lat_lonList1 = clean(lat_lonList1);
            if (lat_lonList1.size() >0 && !lat_lon1.equals("")) {
                for (Object lat_lon:lat_lonList1){
//                    System.out.println(lat_lon+" ***" + lat_lonList1.size());
                    if (computeGeoDistance(lat_lon1, lat_lon.toString())) {
                        isComputeSim = true;
                        break;
                    }
                }
            }
            else
                isComputeSim = true;
            if (isComputeSim) {
                double sim = 0d;
                for (Object label:labelList1){
                    sim += new JaroWinkler(0.7).computeSimilarity(label1, label.toString());
                }
                sim /= labelList1.size();
                if (sim >= threshold)
                    collector.collect(Tuple3.of(input.f0, input.f1, sim));
            }

        }




    }
    public boolean computeGeoDistance(String lat_lon1, String lat_lon2) {
        double lat1 = Double.parseDouble(lat_lon1.split("_")[0]);
        double lat2 = Double.parseDouble(lat_lon2.split("_")[0]);
        double lon1 = Double.parseDouble(lat_lon1.split("_")[1]);
        double lon2 = Double.parseDouble(lat_lon2.split("_")[1]);
        Double arccosParam = Math.sin(lat1) * Math.sin(lat2) + Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon1 - lon2);
        if (arccosParam < -1)
            arccosParam = -1d;
        if (arccosParam > 1)
            arccosParam = 1d;
        double distance = Math.acos(arccosParam) * 6371;
        if (distance <= 1358)
            return true;
        return false;
    }



    private ArrayList clean(ArrayList input){
        ArrayList output = new ArrayList();
        for (Object in: input){
            if (!output.contains(in) && !in.toString().equals(""))
                output.add(in);
        }
        return output;
    }
}