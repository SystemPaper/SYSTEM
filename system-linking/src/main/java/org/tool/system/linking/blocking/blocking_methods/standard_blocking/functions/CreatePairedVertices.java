package org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.omg.CORBA.Object;

import java.util.*;


public class CreatePairedVertices implements GroupCombineFunction<Tuple5<Vertex, String, Long, Boolean, Integer>, Tuple2<Vertex, Vertex>> {

    private boolean intraDatasetComparison;
    private boolean inc_isNewNewCmpr;
    private HashMap<String, HashSet<String>> graphPairs;

    private boolean inc_cmprCnflct;

    public CreatePairedVertices(Boolean intraGraphComparison, boolean inc_isNewNewCmpr,  boolean inc_cmprCnflct, HashMap<String, HashSet<String>> graphPairs){
        this.intraDatasetComparison = intraGraphComparison;
        this.inc_isNewNewCmpr = inc_isNewNewCmpr;
        this.graphPairs = graphPairs;
        this.inc_cmprCnflct = inc_cmprCnflct;
    }


    @Override
    public void combine(Iterable<Tuple5<Vertex, String, Long, Boolean, Integer>> in, Collector<Tuple2<Vertex, Vertex>> out) throws Exception {
        Collection<Tuple2<Vertex,Boolean>> vertices = new ArrayList<>();
        for (Tuple5<Vertex, String, Long, Boolean, Integer> i:in){
            vertices.add(Tuple2.of(i.f0, i.f3));
        }
        Tuple2<Vertex, Boolean>[] verticesArray = vertices.toArray(new Tuple2[vertices.size()]);
        for (int i = 0; i< verticesArray.length && verticesArray [i].f1; i++) {
            for (int j = i + 1; j < verticesArray.length; j++) {
                if (!inc_isNewNewCmpr && !verticesArray[i].f0.hasProperty("ClusterId") && !verticesArray[j].f0.hasProperty("ClusterId")) {
                    continue;
                }
                if (verticesArray[i].f0.hasProperty("ClusterId") && verticesArray[j].f0.hasProperty("ClusterId")) {
                    continue;
                }
//                if (intraDatasetComparison || !verticesArray[i].f0.getGraphIds().containsAny(verticesArray[j].f0.getGraphIds())) {
//                if (!(verticesArray[i].f0.hasProperty("ClusterId") && verticesArray[j].f0.hasProperty("ClusterId"))) {
//
//                if (intraDatasetComparison || !verticesArray[i].f0.getPropertyValue("graphLabel").toString().contains(verticesArray[j].f0.getPropertyValue("graphLabel").toString())) {
//                    if(graphPairs.get("*") != null) {
//                		out.collect(Tuple2.of(verticesArray[i].f0, verticesArray[j].f0));
//                	}


                Vertex v1 = verticesArray[i].f0;
                Vertex v2 = verticesArray[j].f0;

                String sourceGraph = "";
                String targetGraph = "";
                ArrayList graphs = new ArrayList();
//
                if (v1.hasProperty("graphLabel"))
                    sourceGraph = v1.getPropertyValue("graphLabel").getString();
                else
                    graphs.addAll(v1.getPropertyValue("vertices_graphLabel").getList());

                if (v2.hasProperty("graphLabel"))
                    targetGraph = v2.getPropertyValue("graphLabel").getString();
                else
                    graphs.addAll(v2.getPropertyValue("vertices_graphLabel").getList());

                if (!sourceGraph.equals("") && !targetGraph.equals("")) {

                    HashSet<String> allowedGraphs = graphPairs.get(sourceGraph);

                    // allowedGraphs can be null if graph is not used for similarity measuring
                    if (allowedGraphs != null && allowedGraphs.contains(targetGraph)) {
                        out.collect(Tuple2.of(v1, v2));
                    }
                } else {
//
//                        if (sourceGraph.equals(""))
//                            sourceGraph = targetGraph;
//                        HashSet<String> allowedGraphs = graphPairs.get(sourceGraph);
//                        boolean isOut = true;
//                        boolean hasFromSameGraph = false;
//
//                        if (allowedGraphs != null) {
//                            for (java.lang.Object graph : graphs)
//                                if (!allowedGraphs.contains(graph.toString())) {
//                                    if (graph.toString().equals(sourceGraph))
//                                        hasFromSameGraph = true;
//                                    else {
//                                        isOut = false;
//                                        break;
//                                    }
//                                }
//                        }
//                        if (isOut && (!hasFromSameGraph || inc_cmprCnflct))
                    out.collect(Tuple2.of(v1, v2));


                }
//                }//intraDatasetComparison
            }
////        Collection<Tuple2<Vertex,Boolean>> vertices = new ArrayList<>();
////        for (Tuple5<Vertex, String, Long, Boolean, Integer> i:in){
////            vertices.add(Tuple2.of(i.f0, i.f3));
////        }
////        Tuple2<Vertex, Boolean>[] verticesArray = vertices.toArray(new Tuple2[vertices.size()]);
////        for (int i = 0; i< verticesArray.length && verticesArray [i].f1; i++){
////            for (int j = i+1; j< verticesArray.length ; j++){
////                if (!verticesArray[i].f0.getPropertyValue("graphLabel").equals(verticesArray[j].f0.getPropertyValue("graphLabel")) || intraDatasetComparison)
////                    out.collect(Tuple2.of(verticesArray[i].f0, verticesArray[j].f0));
////            }
////        }
//
//
//
        }
    }
}


