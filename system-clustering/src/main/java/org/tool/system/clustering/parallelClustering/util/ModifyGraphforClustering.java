package org.tool.system.clustering.parallelClustering.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

/**
 * Used to assign Vertex Priority to vertices.
 * It must be used before applying org.tool.system.clustering.clustering algorithms on graphs
 * Notice: VertexPriority must not be 0
 */
public class ModifyGraphforClustering implements UnaryGraphToGraphOperator, UnaryCollectionToCollectionOperator {
    public String getName() {
        return ModifyGraphforClustering.class.getName();
    }
    public DataSet<Vertex> execute(DataSet<Vertex> vertices) {
        vertices = DataSetUtils.zipWithUniqueId(vertices).map(new MapFunction<Tuple2<Long, Vertex>, Vertex>() {
            public Vertex map(Tuple2<Long, Vertex> in) throws Exception {
                in.f1.setProperty("VertexPriority", in.f0+1);
                return in.f1;
            }
        });
        return vertices;
    }

    @Override
    public GraphCollection execute(GraphCollection graphCollection) {
        return graphCollection.getConfig().getGraphCollectionFactory()
                .fromDataSets(graphCollection.getGraphHeads(), execute (graphCollection.getVertices()), graphCollection.getEdges() );
    }
    @Override
    public LogicalGraph execute(LogicalGraph graph) {
        return graph.getConfig().getLogicalGraphFactory()
                .fromDataSets(graph.getGraphHead(), execute (graph.getVertices()), graph.getEdges() );    }
}
