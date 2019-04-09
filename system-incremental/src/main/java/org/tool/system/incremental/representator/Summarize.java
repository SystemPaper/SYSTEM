package org.tool.system.incremental.representator;


import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.tool.system.incremental.representator.func.MyAggregateListOfWccVertices;
import org.tool.system.incremental.representator.func.MySumProperty;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;

public class Summarize implements UnaryCollectionToCollectionOperator {
    private String vertexGroupingKey;
    private String[] wccPropertyKeyList;
    private Boolean[] isRepetitionAllowed;
    private Boolean isEdgeAggregation;

    public Summarize (String vertexGroupingKey, String[] wccPropertyKeyList, Boolean[] isRepetitionAllowed, Boolean isEdgeAggregation){
        this.vertexGroupingKey = vertexGroupingKey;
        this.wccPropertyKeyList = wccPropertyKeyList;
        this.isRepetitionAllowed = isRepetitionAllowed;
        this.isEdgeAggregation = isEdgeAggregation;
    }
    @Override
    public GraphCollection execute(GraphCollection input) {

        LogicalGraph inputGraph = input.getConfig().getLogicalGraphFactory()
                .fromDataSets(input.getVertices(), input.getEdges());

        Grouping.GroupingBuilder groupingBuilder = new Grouping.GroupingBuilder()
                .setStrategy(GroupingStrategy.GROUP_REDUCE)
                .addVertexGroupingKey(vertexGroupingKey);

        for (int i=0; i< wccPropertyKeyList.length; i++){
            groupingBuilder.addVertexAggregateFunction(new MyAggregateListOfWccVertices(wccPropertyKeyList[i], isRepetitionAllowed[i]));
        }
        if(isEdgeAggregation){
            groupingBuilder
                    .addEdgeAggregateFunction(new MySumProperty("value"))
                    .addEdgeAggregateFunction(new Count());

        }



        LogicalGraph summarizedGraph = groupingBuilder.build().execute(inputGraph);


        DataSet<Edge> edges = summarizedGraph.getEdges();
//        DataSet<Edge> edges = summarizedGraph.getEdges().map(new MapFunction<Edge, Edge>() {
//            @Override
//            public Edge map(Edge edge) throws Exception {
//                if(edge.getSourceId().compareTo(edge.getTargetId()) >= 0) {
//                    GradoopId sourceId = edge.getSourceId();
//                    GradoopId targetId = edge.getTargetId();
//                    edge.setSourceId(targetId);
//                    edge.setTargetId(sourceId);
//                }
//                edge.setProperty("value",edge.getPropertyValue("sum_value"));
//                edge.removeProperty("sum_value");
//                return edge;
//            }
//        });

        return input.getConfig().getGraphCollectionFactory()
                .fromDataSets(summarizedGraph.getGraphHead(), summarizedGraph.getVertices(), edges);
    }

    @Override
    public String getName() {
        return this.getName();
    }
}
