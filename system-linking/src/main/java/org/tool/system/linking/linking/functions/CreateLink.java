package org.tool.system.linking.linking.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 */
public class CreateLink implements MapFunction<Tuple3<Vertex, Vertex, Double>, Edge>{
    private EdgeFactory edgeFactory;
    private String edgeLabel;
    public CreateLink(EdgeFactory EdgeFactory, String EdgeLabel){
        edgeFactory = EdgeFactory;
        edgeLabel = EdgeLabel;
    }
    @Override
    public Edge map(Tuple3<Vertex, Vertex, Double> input) throws Exception {


      Edge edge;
        if(input.f0.getId().compareTo(input.f1.getId()) < 0)
            edge = edgeFactory.createEdge(input.f0.getId(), input.f1.getId());
        else
            edge = edgeFactory.createEdge(input.f1.getId(), input.f0.getId());


//        GradoopIdSet graphIds = input.f0.getGraphIds();
//        for(GradoopId id : input.f1.getGraphIds()){
//            if (!graphIds.contains(id))
//                graphIds.add(id);
//        }


        GradoopIdSet graphIds = new GradoopIdSet();
        graphIds.add(edge.getId());
        edge.setGraphIds(graphIds);

        /////////////////////////

        edge.setProperty(edgeLabel, input.f2);
        if(input.f0.hasProperty("inc") && input.f1.hasProperty("inc"))
            if (input.f0.getPropertyValue("inc").toString().equals(input.f1.getPropertyValue("inc").toString()))
                edge.setProperty("new","new");
        return edge;
    }
}
