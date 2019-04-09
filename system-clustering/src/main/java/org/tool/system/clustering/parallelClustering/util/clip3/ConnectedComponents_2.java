package org.tool.system.clustering.parallelClustering.util.clip3;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.Graph;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 *
 */

//DataSet<Tuple3<Edge, Tuple2<String, String>, Tuple2<String, String>>> link_srcVertex_targetVertex =
//        new Link2Link_SrcVertex_TrgtVertex2(Vid_src, input.getEdges()).execute();

public class ConnectedComponents_2 {
    private DataSet<Tuple2<String, String>> vertices;
    private DataSet<Edge> edges;
    private String prefix;
    public ConnectedComponents_2(DataSet<Tuple2<String, String>> vertices, DataSet<Edge> edges, String prefix){
        this.vertices = vertices;
        this.edges = edges;
        this.prefix = prefix;
    }
//vid_vSrc_ClusterId
    public DataSet<Tuple3<String, String, String>> execute () throws Exception {

        DataSet<Tuple3<String, String, Long>> v_vp = DataSetUtils.zipWithUniqueId(vertices).map(new MapFunction<Tuple2<Long, Tuple2<String, String>>,
                Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(Tuple2<Long, Tuple2<String, String>> in) throws Exception {
                return Tuple3.of(in.f1.f0, in.f1.f1, in.f0);
            }
        });


            Graph gellyGraph = Graph.fromDataSet(
                    v_vp.map(new ToGellyVertexWithIdValue()),
                    edges.flatMap(new ToGellyEdgeforSGInput()),
                    ExecutionEnvironment.getExecutionEnvironment()
            );

            DataSet<org.apache.flink.graph.Vertex<String, Long>> clusteredVertices =
                    new org.apache.flink.graph.library.ConnectedComponents(Integer.MAX_VALUE)
                    .run(gellyGraph);

            DataSet<Tuple3<String, String, String>> res =
                    clusteredVertices.map(new GetVertices(prefix));
            return res;


    }


    private class ToGellyVertexWithIdValue implements MapFunction<Tuple3<String, String, Long>, org.apache.flink.graph.Vertex<String, Long>> {
        @Override
        public org.apache.flink.graph.Vertex<String, Long> map(Tuple3<String, String, Long> in) throws Exception {
            return new org.apache.flink.graph.Vertex<String, Long>(in.f0+"-"+in.f1, in.f2);
        }
    }

    private class ToGellyEdgeforSGInput implements
            org.apache.flink.api.common.functions.FlatMapFunction<Edge, org.apache.flink.graph.Edge<String, Double>> {
        @Override
        public void flatMap(Edge e, Collector<org.apache.flink.graph.Edge<String, Double>> collector) throws Exception {
            collector.collect(new org.apache.flink.graph.Edge
                    (e.getSourceId().toString(), e.getTargetId().toString(), 0.0));
        }
    }

    private class GetVertices implements MapFunction<org.apache.flink.graph.Vertex<String, Long>, Tuple3<String, String, String>> {
        private String prefix;
        public GetVertices(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Tuple3<String, String, String> map(org.apache.flink.graph.Vertex<String, Long> in) throws Exception {
            return Tuple3.of(in.f0.split("-")[0], in.f0.split("-")[1], prefix+in.f1);
        }
    }
//
//    private class GetVertices implements org.apache.flink.api.common.functions.MapFunction<org.apache.flink.graph.Vertex<String, Long>, R> {
//    }
}
