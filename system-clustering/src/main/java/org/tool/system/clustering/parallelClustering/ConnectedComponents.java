package org.tool.system.clustering.parallelClustering;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.Edge;

import org.tool.system.clustering.parallelClustering.util.ClusteringOutputType;
import org.tool.system.clustering.parallelClustering.util.ModifyGraphforClustering;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.Serializable;
/**
 * The implementation of Connected Component algorithm.
 */

public class ConnectedComponents
        implements UnaryGraphToGraphOperator, UnaryCollectionToCollectionOperator, Serializable{

    
    public String getName() {
        // TODO Auto-generated method stub
        return ConnectedComponents.class.getName();
    }
    private String clusterIdPrefix;
    private ClusteringOutputType clusteringOutputType;
    private String clsTitle ="";
    public ConnectedComponents(){clusterIdPrefix=""; clusteringOutputType = ClusteringOutputType.GRAPH;}
    public ConnectedComponents(String prefix){clusterIdPrefix = prefix; clusteringOutputType = ClusteringOutputType.GRAPH;}
    public ConnectedComponents(String prefix, ClusteringOutputType clusteringOutputType){clusterIdPrefix = prefix;this.clusteringOutputType = clusteringOutputType;}

    public ConnectedComponents(String clsTitle, String clusterIdPrefix)
    {clusterIdPrefix=""; clusteringOutputType = ClusteringOutputType.GRAPH; this.clsTitle = clsTitle;}

    @Override
    public GraphCollection execute(GraphCollection graphCollection) {
        graphCollection = graphCollection.callForCollection(new ModifyGraphforClustering());

        GraphCollection resultGC = graphCollection.getConfig().getGraphCollectionFactory().fromDataSets
                (graphCollection.getGraphHeads(),
                        cluster (graphCollection.getVertices(), graphCollection.getEdges(), graphCollection.getConfig()), graphCollection.getEdges());

        return resultGC;
    }
    public LogicalGraph execute(LogicalGraph graph) {
        graph = graph.callForGraph(new ModifyGraphforClustering());

        LogicalGraph resultLG = graph.getConfig().getLogicalGraphFactory().fromDataSets
                (graph.getGraphHead(), cluster (graph.getVertices(), graph.getEdges(), graph.getConfig()), graph.getEdges());

        return resultLG;
    }


    public DataSet<Vertex> cluster(DataSet<Vertex> vertices, DataSet<Edge> edges, GradoopFlinkConfig config) {


        try {

            Graph gellyGraph = Graph.fromDataSet(
                   vertices.map(new ToGellyVertexWithIdValue()),
                    edges.flatMap(new ToGellyEdgeforSGInput()),
                    config.getExecutionEnvironment()
            );

            DataSet<org.apache.flink.graph.Vertex<GradoopId, Long>> ResVertices = new org.apache.flink.graph.library.ConnectedComponents(Integer.MAX_VALUE)
                    .run(gellyGraph);

            DataSet<Vertex> lgVertices= ResVertices.join(vertices.map(new toVertexAndGradoopId())).where(0).equalTo(0)
                    .with(new joinFunc(clusterIdPrefix));
            return lgVertices;




        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


        return null;
    }




    public static final class ToGellyVertexWithIdValue
            implements MapFunction<Vertex, org.apache.flink.graph.Vertex<GradoopId, Long>> {
        
        public org.apache.flink.graph.Vertex map(Vertex in) throws Exception {
//            in.setProperty("VertexPriority", Long.parseLong(in.getPropertyValue("id").toString()));
            Long vp = Long.parseLong(in.getPropertyValue("VertexPriority").toString());
//            in.setProperty("ClusterId", 0);
//            in.setProperty("roundNo", 0);
            GradoopId id = in.getId();
            return new org.apache.flink.graph.Vertex<GradoopId, Long>(id, vp);
        }
    }
    public static final class toVertexAndGradoopId
            implements MapFunction<Vertex, Tuple2<GradoopId, Vertex>> {
        
        public Tuple2<GradoopId, Vertex> map(Vertex in) throws Exception {
            return Tuple2.of(in.getId(),in);
        }
    }

    public class ToGellyEdgeforSGInput<E extends EPGMEdge>
            implements FlatMapFunction<E, org.apache.flink.graph.Edge<GradoopId, Double>> {
        
        public void flatMap(E e, Collector<org.apache.flink.graph.Edge
                <GradoopId, Double>> out) throws Exception {
            out.collect(new org.apache.flink.graph.Edge
                    (e.getSourceId(), e.getTargetId(), 0.0));
        }
    }
    public class joinFunc implements JoinFunction <org.apache.flink.graph.Vertex<GradoopId, Long>, Tuple2<GradoopId, Vertex>, Vertex>{
        private String clusterIdPrefix;
        public joinFunc(String prefix){clusterIdPrefix = prefix;}
        public Vertex join(org.apache.flink.graph.Vertex<GradoopId, Long> in1, Tuple2<GradoopId, Vertex> in2) {
            if(clsTitle.equals(""))
                clsTitle = "ClusterId";
            in2.f1.setProperty(clsTitle,clusterIdPrefix+in1.f1);
            return in2.f1;
        }
    }

}
