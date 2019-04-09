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
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSink;
import org.gradoop.flink.io.impl.deprecated.json.JSONDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.tool.system.example.EntityConfigMaker.util.CorrectIncId;
import org.tool.system.example.EntityConfigMaker.util.FilterByIncId;
import org.tool.system.example.EntityConfigMaker.util.GetSrc;
import org.tool.system.example.EntityConfigMaker.util.SetIncId;

public class Partitioner_P2 {
    public static void main(String args[]) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        for(int confId =1; confId<=4; confId++) {

            String inputPath = args[0];
            String outputPath = args[1]+confId+"/";


            CSVDataSource dataSource =
                new CSVDataSource(inputPath , config);
            GraphCollection input = dataSource.getGraphCollection();



    DataSet<Vertex> src1 = input.getVertices().flatMap(new GetSrc("1"));
    DataSet<Vertex> src2 = input.getVertices().flatMap(new GetSrc("2"));
    DataSet<Vertex> src3 = input.getVertices().flatMap(new GetSrc("3"));
    DataSet<Vertex> src4 = input.getVertices().flatMap(new GetSrc("4"));
    DataSet<Vertex> src5 = input.getVertices().flatMap(new GetSrc("5"));

    DataSet<Vertex> src6 = input.getVertices().flatMap(new GetSrc("6"));
    DataSet<Vertex> src7 = input.getVertices().flatMap(new GetSrc("7"));
    DataSet<Vertex> src8 = input.getVertices().flatMap(new GetSrc("8"));
    DataSet<Vertex> src9 = input.getVertices().flatMap(new GetSrc("9"));
    DataSet<Vertex> src10 = input.getVertices().flatMap(new GetSrc("10"));
            env.setParallelism(1);
    DataSet<Tuple2<Long, Vertex>> id_vertex1 = DataSetUtils.zipWithUniqueId(src1);
    DataSet<Tuple2<Long, Vertex>> id_vertex2 = DataSetUtils.zipWithUniqueId(src2);
    DataSet<Tuple2<Long, Vertex>> id_vertex3 = DataSetUtils.zipWithUniqueId(src3);
    DataSet<Tuple2<Long, Vertex>> id_vertex4 = DataSetUtils.zipWithUniqueId(src4);
    DataSet<Tuple2<Long, Vertex>> id_vertex5 = DataSetUtils.zipWithUniqueId(src5);

    DataSet<Tuple2<Long, Vertex>> id_vertex6 = DataSetUtils.zipWithUniqueId(src6);
    DataSet<Tuple2<Long, Vertex>> id_vertex7 = DataSetUtils.zipWithUniqueId(src7);
    DataSet<Tuple2<Long, Vertex>> id_vertex8 = DataSetUtils.zipWithUniqueId(src8);
    DataSet<Tuple2<Long, Vertex>> id_vertex9 = DataSetUtils.zipWithUniqueId(src9);
    DataSet<Tuple2<Long, Vertex>> id_vertex10 = DataSetUtils.zipWithUniqueId(src10);




    DataSet<Vertex> vertices = null;

    switch (confId) {
        case 1:
            vertices = id_vertex1.map(new SetIncId(5, 0)).union(
                    id_vertex2.map(new SetIncId(5, 0))).union(
                    id_vertex3.map(new SetIncId(5, 0))).union(
                    id_vertex5.map(new SetIncId(5, 0))).union(
                    id_vertex4.map(new SetIncId(5, 0)));

            DataSet<Vertex> vertices2 = id_vertex6.map(new SetIncId(5, 0)).union(
                    id_vertex7.map(new SetIncId(5, 0))).union(
                    id_vertex8.map(new SetIncId(5, 0))).union(
                    id_vertex9.map(new SetIncId(5, 0))).union(
                    id_vertex10.map(new SetIncId(5, 0)));
            vertices = vertices2.union(vertices);
            break;
        case 2:
            vertices = id_vertex1.map(new SetIncId(3, 0)).union(
                    id_vertex2.map(new SetIncId(3, 0))).union(
                    id_vertex3.map(new SetIncId(3, 0))).union(
                    id_vertex5.map(new SetIncId(3, 0))).union(
                    id_vertex4.map(new SetIncId(3, 0)));

            vertices2 = id_vertex6.map(new SetIncId(3, 0)).union(
                    id_vertex7.map(new SetIncId(3, 0))).union(
                    id_vertex8.map(new SetIncId(3, 0))).union(
                    id_vertex9.map(new SetIncId(3, 0))).union(
                    id_vertex10.map(new SetIncId(3, 0)));
            vertices = vertices2.union(vertices);

            break;
        case 3:
            vertices = id_vertex1.map(new SetIncId(2, 0)).union(
                    id_vertex2.map(new SetIncId(2, 0))).union(
                    id_vertex3.map(new SetIncId(2, 0))).union(
                    id_vertex5.map(new SetIncId(2, 0))).union(
                    id_vertex4.map(new SetIncId(2, 0)));

            vertices2 = id_vertex6.map(new SetIncId(2, 0)).union(
                    id_vertex7.map(new SetIncId(2, 0))).union(
                    id_vertex8.map(new SetIncId(2, 0))).union(
                    id_vertex9.map(new SetIncId(2, 0))).union(
                    id_vertex10.map(new SetIncId(2, 0)));
            vertices = vertices2.union(vertices);


            DataSet<Vertex> vertices0 = vertices.flatMap(new FilterByIncId(0));
            DataSet<Vertex> vertices1 = vertices.flatMap(new FilterByIncId(1));



            src1 = vertices1.flatMap(new GetSrc("1"));
            src2 = vertices1.flatMap(new GetSrc("2"));
            src3 = vertices1.flatMap(new GetSrc("3"));
            src4 = vertices1.flatMap(new GetSrc("4"));
            src5 = vertices1.flatMap(new GetSrc("5"));


            src6 = vertices1.flatMap(new GetSrc("6"));
            src7 = vertices1.flatMap(new GetSrc("7"));
            src8 = vertices1.flatMap(new GetSrc("8"));
            src9 = vertices1.flatMap(new GetSrc("9"));
            src10 = vertices1.flatMap(new GetSrc("10"));


            id_vertex1 = DataSetUtils.zipWithUniqueId(src1);
            id_vertex2 = DataSetUtils.zipWithUniqueId(src2);
            id_vertex3 = DataSetUtils.zipWithUniqueId(src3);
            id_vertex4 = DataSetUtils.zipWithUniqueId(src4);
            id_vertex5 = DataSetUtils.zipWithUniqueId(src5);


            id_vertex6 = DataSetUtils.zipWithUniqueId(src6);
            id_vertex7 = DataSetUtils.zipWithUniqueId(src7);
            id_vertex8 = DataSetUtils.zipWithUniqueId(src8);
            id_vertex9 = DataSetUtils.zipWithUniqueId(src9);
            id_vertex10 = DataSetUtils.zipWithUniqueId(src10);



            vertices1 = id_vertex1.map(new SetIncId(5, 1)).union(
                    id_vertex2.map(new SetIncId(5, 1))).union(
                    id_vertex3.map(new SetIncId(5, 1))).union(
                    id_vertex5.map(new SetIncId(5, 1))).union(
                    id_vertex4.map(new SetIncId(5, 1)));

            vertices1 = vertices1.union(id_vertex6.map(new SetIncId(5, 1)).union(
                    id_vertex7.map(new SetIncId(5, 1))).union(
                    id_vertex8.map(new SetIncId(5, 1))).union(
                    id_vertex9.map(new SetIncId(5, 1))).union(
                    id_vertex10.map(new SetIncId(5, 1))));



            vertices = vertices0.union(vertices1);
            break;
        case 4:

            vertices = id_vertex1.map(new SetIncId(10, 0)).union(
                    id_vertex2.map(new SetIncId(10, 0))).union(
                    id_vertex3.map(new SetIncId(10, 0))).union(
                    id_vertex5.map(new SetIncId(10, 0))).union(
                    id_vertex4.map(new SetIncId(10, 0)));


            vertices2 = id_vertex10.map(new SetIncId(10, 0)).union(
                    id_vertex6.map(new SetIncId(10, 0))).union(
                    id_vertex7.map(new SetIncId(10, 0))).union(
                    id_vertex8.map(new SetIncId(10, 0))).union(
                    id_vertex9.map(new SetIncId(10, 0)));

            vertices = vertices.union(vertices2);
            vertices = vertices.map(new CorrectIncId());
            break;
    }

    GraphCollection result = config.getGraphCollectionFactory().fromDataSets(input.getGraphHeads(), vertices);

    CSVDataSink jss = new CSVDataSink(outputPath ,input.getConfig());
    jss.write(result, true);

    env.execute();




}



    }





}