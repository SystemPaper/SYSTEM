package org.tool.system.example.EntityConfigMaker;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

public class Partitioner_P1 {
    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        String outputPath = args[1];

        for(int confId =1; confId<=4; confId++) {

            String inputPath = args[0];

            CSVDataSource dataSource =
            new CSVDataSource(inputPath , config);
            GraphCollection input = dataSource.getGraphCollection();


            DataSet<Vertex> src1 = input.getVertices().flatMap(new GetSrc("1"));
            DataSet<Vertex> src2 = input.getVertices().flatMap(new GetSrc("2"));
            DataSet<Vertex> src3 = input.getVertices().flatMap(new GetSrc("3"));
            DataSet<Vertex> src4 = input.getVertices().flatMap(new GetSrc("4"));
            DataSet<Vertex> src5 = input.getVertices().flatMap(new GetSrc("5"));

            env.setParallelism(1);

            DataSet<Tuple2<Long, Vertex>> id_vertex1 = DataSetUtils.zipWithUniqueId(src1);
            DataSet<Tuple2<Long, Vertex>> id_vertex2 = DataSetUtils.zipWithUniqueId(src2);
            DataSet<Tuple2<Long, Vertex>> id_vertex3 = DataSetUtils.zipWithUniqueId(src3);
            DataSet<Tuple2<Long, Vertex>> id_vertex4 = DataSetUtils.zipWithUniqueId(src4);
            DataSet<Tuple2<Long, Vertex>> id_vertex5 = DataSetUtils.zipWithUniqueId(src5);




    DataSet<Vertex> vertices = null;

    switch (confId) {
        case 1:
            vertices = id_vertex1.map(new SetIncId(5, 0)).union(
                    id_vertex2.map(new SetIncId(5, 0))).union(
                    id_vertex3.map(new SetIncId(5, 0))).union(
                    id_vertex5.map(new SetIncId(5, 0))).union(
                    id_vertex4.map(new SetIncId(5, 0)));
            break;
        case 2:
            vertices = id_vertex1.map(new SetIncId(3, 0)).union(
                    id_vertex2.map(new SetIncId(3, 0))).union(
                    id_vertex3.map(new SetIncId(3, 0))).union(
                    id_vertex5.map(new SetIncId(3, 0))).union(
                    id_vertex4.map(new SetIncId(3, 0)));
            break;
        case 3:
            vertices = id_vertex1.map(new SetIncId(2, 0)).union(
                    id_vertex2.map(new SetIncId(2, 0))).union(
                    id_vertex3.map(new SetIncId(2, 0))).union(
                    id_vertex5.map(new SetIncId(2, 0))).union(
                    id_vertex4.map(new SetIncId(2, 0)));
            DataSet<Vertex> vertices0 = vertices.flatMap(new FilterByIncId(0));
            DataSet<Vertex> vertices1 = vertices.flatMap(new FilterByIncId(1));


            src1 = vertices1.flatMap(new GetSrc("1"));
            src2 = vertices1.flatMap(new GetSrc("2"));
            src3 = vertices1.flatMap(new GetSrc("3"));
            src4 = vertices1.flatMap(new GetSrc("4"));
            src5 = vertices1.flatMap(new GetSrc("5"));

            id_vertex1 = DataSetUtils.zipWithUniqueId(src1);
            id_vertex2 = DataSetUtils.zipWithUniqueId(src2);
            id_vertex3 = DataSetUtils.zipWithUniqueId(src3);
            id_vertex4 = DataSetUtils.zipWithUniqueId(src4);
            id_vertex5 = DataSetUtils.zipWithUniqueId(src5);

            vertices1 = id_vertex1.map(new SetIncId(5, 1)).union(
                    id_vertex2.map(new SetIncId(5, 1))).union(
                    id_vertex3.map(new SetIncId(5, 1))).union(
                    id_vertex5.map(new SetIncId(5, 1))).union(
                    id_vertex4.map(new SetIncId(5, 1)));

            vertices = vertices0.union(vertices1);
            break;
        case 4:

            vertices = id_vertex1.map(new SetIncId(10, 0)).union(
                    id_vertex2.map(new SetIncId(10, 0))).union(
                    id_vertex3.map(new SetIncId(10, 0))).union(
                    id_vertex5.map(new SetIncId(10, 0))).union(
                    id_vertex4.map(new SetIncId(10, 0)));
            vertices = vertices.map(new CorrectIncId());
            break;
    }

    GraphCollection result = config.getGraphCollectionFactory().fromDataSets(input.getGraphHeads(), vertices);
    String finalOutputPath = outputPath+ "conf"+confId+"/";

    CSVDataSink csvDataSink = new CSVDataSink(finalOutputPath , config);
    csvDataSink.write(result, true);
    env.execute();
}



    }

    private static class SetIncId implements MapFunction<Tuple2<Long, Vertex>, Vertex> {
        private int inc;
        private int plus;
        public SetIncId(int inc, int plus) {
            this.inc = inc;
            this.plus = plus;
        }

        @Override
        public Vertex map(Tuple2<Long, Vertex> id_vertex) throws Exception {
            id_vertex.f1.setProperty("inc",(id_vertex.f0%inc)+plus);
            return id_vertex.f1;
        }
    }

    private static class FilterByIncId implements FlatMapFunction<Vertex, Vertex> {
        int incId;
        public FilterByIncId(int i) {
            incId = i;
        }

        @Override
        public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
            if (Integer.parseInt(vertex.getPropertyValue("inc").toString()) == incId)
                collector.collect(vertex);
        }
    }

    private static class CorrectIncId implements MapFunction<Vertex, Vertex> {
        @Override
        public Vertex map(Vertex vertex) throws Exception {
            int incId = Integer.parseInt(vertex.getPropertyValue("inc").toString());
            if (incId == 0)
                vertex.setProperty("inc",1);
            else if (incId == 1)
                vertex.setProperty("inc", 2);
            else
                vertex.setProperty("inc", 0);
            return vertex;
        }
    }

    private static class GetSrc implements FlatMapFunction<Vertex, Vertex> {
        String src;
        public GetSrc(String src) {
            this.src = src;
        }

        @Override
        public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
            if (vertex.getPropertyValue("graphLabel").toString().equals(src))
                collector.collect(vertex);
        }
    }
}