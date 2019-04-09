package org.tool.system.linking.similarity_measuring;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.linking.similarity_measuring.data_structures.EmbeddingComponent;
import org.tool.system.linking.similarity_measuring.data_structures.SimilarityComponent;
import org.tool.system.linking.similarity_measuring.data_structures.SimilarityFieldList;

import java.io.File;
import java.util.Collection;
import java.util.Optional;


public class  SimilarityMeasurer extends RichFlatMapFunction<Tuple2<Vertex, Vertex>, Tuple3<Vertex, Vertex, SimilarityFieldList>> {

    private Collection<SimilarityComponent> similarityComponents;

    public SimilarityMeasurer(Collection<SimilarityComponent> SimilarityComponents) {
        similarityComponents = SimilarityComponents;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // Open vectors filed registered in Flink's distributed cache
        similarityComponents.stream()
                .filter(c -> c instanceof EmbeddingComponent)
                .forEach(c -> {
                    EmbeddingComponent component = (EmbeddingComponent) c;
                    File vectorFile = getRuntimeContext().getDistributedCache().getFile(component.getDistributedCacheKey());
                    component.setWordVectorsFile(vectorFile);
                });
    }

    @Override
    public void flatMap(Tuple2<Vertex, Vertex> input, Collector<Tuple3<Vertex, Vertex, SimilarityFieldList>> out) throws Exception {
        SimilarityFieldList similarityFieldList = new SimilarityFieldList();
        for (SimilarityComponent sc : similarityComponents) {

            Optional<Tuple2<String, String>> pair = getValuePair(input, sc);
            if (pair.isPresent()) {
                similarityFieldList.add(new SimilarityComputer(sc).computeSimilarity(pair.get().f0, pair.get().f1));
            }
        }

        if (!similarityFieldList.isEmpty()) {
            out.collect(Tuple3.of(input.f0, input.f1, similarityFieldList));
        }
    }

    private Optional<Tuple2<String, String>> getValuePair(Tuple2<Vertex, Vertex> input, SimilarityComponent sc) {
        Tuple2<String, String> valuePair = null;
//        System.out.println(isSrcTargetPair(input.f0, input.f1, sc));
        String src = "";
        String trgt = "";
        if (input.f0.getPropertyValue(sc.getSrcAttribute()) != null)
            src = input.f0.getPropertyValue(sc.getSrcAttribute()).toString();
        if (input.f1.getPropertyValue(sc.getTargetAttribute()) != null)
            trgt = input.f1.getPropertyValue(sc.getTargetAttribute()).toString();

        if (isSrcTargetPair(input.f0, input.f1, sc)) {
            valuePair = new Tuple2<>(src, trgt);

        } else if (isSrcTargetPair(input.f1, input.f0, sc)) {
            valuePair = new Tuple2<>(trgt, src);

        }
        return Optional.ofNullable(valuePair);
    }

    private Boolean isSrcTargetPair(Vertex src, Vertex tar, SimilarityComponent sc) {

        if(sc.getSrcGraphLabel().equals(getGraphLabel(tar)) &&
                sc.getTargetGraphLabel().equals(getGraphLabel(src))) {
            Vertex temp = src;
            src = tar;
            tar = temp;
        }

        return isGraphlabelMatch(getGraphLabel(src), sc.getSrcGraphLabel())
                && isGraphlabelMatch(getGraphLabel(tar), sc.getTargetGraphLabel())
//                && src.getLabel().equals(sc.getSrcLabel())
//                && tar.getLabel().equals(sc.getTargetLabel())
                && src.hasProperty(sc.getSrcAttribute())
                && tar.hasProperty(sc.getTargetAttribute());
    }

    private String getGraphLabel(Vertex v) {
        return v.getPropertyValue("graphLabel").toString();
    }

    private Boolean isGraphlabelMatch(String cur, String ref) {
        return ref.equals("*") || ref.equals(cur);
    }
}
