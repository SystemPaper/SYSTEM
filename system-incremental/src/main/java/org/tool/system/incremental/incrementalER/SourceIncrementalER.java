package org.tool.system.incremental.incrementalER;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.tool.system.clustering.parallelClustering.ClusteringGC;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPConfig;
import org.tool.system.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.tool.system.incremental.repairer.methods.*;
import org.tool.system.incremental.repairer.util.RepairMethod;
import org.tool.system.incremental.representator.Summarize;
import org.tool.system.incremental.temp.RunConfig;
import org.tool.system.inputConfig.LinkerConfig;
import org.tool.system.linking.linking.Linker;
import org.tool.system.linking.linking.data_structures.LinkerComponent;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SourceIncrementalER {
    public void execute(String confFilePath, int runId, double threshold, String datset) throws Exception {
        RunConfig runConfig = new RunConfig().readConfig(confFilePath, runId);

//        Double aggThreshold = runConfig.getAggThreshold();
        Double aggThreshold = threshold;
        String linkingConfFilePath = runConfig.getLinkingConfFilePath();
        int incrementLength = runConfig.getIncrementLength();
        String initialCollectionPath = runConfig.getInitialCollectionPath();
        String tempOutPath = runConfig.getTempOutPath();
        RepairMethod repairMethod = runConfig.getRepairMethod();
        String resOutPath = runConfig.getResOutPath();
        String vertexLabel = runConfig.getVertexLabel();
        String resOutInfo = runConfig.getResOutInfo();
        String[] srcNames = runConfig.getSrcNames();


        String vertexGroupingKey = runConfig.getVertexGroupingKey();
        String[] wccPropertyKeyList = runConfig.getWccPropertyKeyList().split(",");

        Boolean[] isRepetitionAllowed = runConfig.getRepetitionAllowed();


        String linkingRunTimes = "";
        String clusteringRunTimes = "";




        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        LinkerConfig linkerConfig = new LinkerConfig(aggThreshold);
        List<LinkerComponent> linkerComponents = linkerConfig.readConfig(linkingConfFilePath);

//Iteration 1


        /* Step : increment (get vertices) */
        String inputPath = initialCollectionPath+srcNames[0]+"/";
        GraphCollection srcGC = null;

            CSVDataSource src = new CSVDataSource(inputPath, config);
            srcGC = src.getGraphCollection();



        GraphCollection current = srcGC;


        inputPath = initialCollectionPath+srcNames[1]+"/";

            src = new CSVDataSource(inputPath, config);
            srcGC = src.getGraphCollection();



        current = config.getGraphCollectionFactory().fromDataSets(srcGC.getGraphHeads(), current.getVertices().union(srcGC.getVertices()));
        current = current.callForCollection(new ClearGraphIds(GradoopId.get()));



        /* Step 3: linking  */


        LinkerComponent linkerComponent = linkerComponents.get(0);
        Linker linker = new Linker(linkerComponent, datset);
        current = current.callForCollection(linker);
        current = current.callForCollection(new ClearGraphIds(GradoopId.get()));


        String outputPath = tempOutPath+"linking0/";

            CSVDataSink dataSink = new CSVDataSink(outputPath , current.getConfig());
        dataSink.write(current, true);

        env.execute(resOutInfo+"---"+"inc0-linking "+threshold);
        linkingRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");


            CSVDataSource dataSource =
                    new CSVDataSource(outputPath , config);
            current = dataSource.getGraphCollection();


        /* Step 4: clustering  */

        CLIPConfig clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(2);
        current = current.callForCollection(new ClusteringGC(clipConfig));

        if (!vertexGroupingKey.equals(""))
            current = current.callForCollection(new Summarize(vertexGroupingKey, wccPropertyKeyList, isRepetitionAllowed, true));

        current = current.callForCollection(new ClearGraphIds(GradoopId.get()));



        outputPath = tempOutPath+"clustering0/";

        dataSink = new CSVDataSink(outputPath , current.getConfig());
        dataSink.write(current, true);

        env.execute(resOutInfo+"---"+"inc0-clustering "+threshold);
        clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");
        ////////////////////////////////////////////////////////////////////////
//Iteration 3+
        int srcNo =3;
        incrementLength--;
        for(int i=1;i< incrementLength;i++) {

                dataSource =
                        new CSVDataSource(outputPath , config);
                current = dataSource.getGraphCollection();



        /* Step 1: increment (get vertices) */
            inputPath = initialCollectionPath+srcNames[i+1]+"/";


                src = new CSVDataSource(inputPath, config);
                srcGC = src.getGraphCollection();

 /* Step 2: add the the next graph  */
            current = config.getGraphCollectionFactory().fromDataSets(srcGC.getGraphHeads(), srcGC.getVertices().union(current.getVertices()));
            current = current.callForCollection(new ClearGraphIds(GradoopId.get()));

        /* Step 3: linking  */
            linkerComponent = linkerComponents.get(0);
            linker = new Linker(linkerComponent, datset);
            current = current.callForCollection(linker);
            current = current.callForCollection(new ClearGraphIds(GradoopId.get()));

            outputPath = tempOutPath+"linking"+i+"/";

                dataSink = new CSVDataSink(outputPath , current.getConfig());
            dataSink.write(current, true);

            env.execute(resOutInfo+"---"+"inc"+i+"-linking "+threshold);
            linkingRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");


                dataSource =
                        new CSVDataSource(outputPath , config);
                current = dataSource.getGraphCollection();



        /* Step 4: repairing  */
            clipConfig = new CLIPConfig();
            clipConfig.setSourceNo(srcNo);

            switch (repairMethod){
                case EMB:
//                    current = current.callForGraph(new EarlyMaxBoth(srcNo,i+"", false, false));
                    current = current.callForCollection(new AdvancedEarlyMaxBoth(srcNo,i+"", false, false));

                    break;
                case LMB:
                    current = current.callForCollection(new AdvancedLateMaxBoth(srcNo,i+"",false));
                    break;
                case ITERATIVE:
//                case IT:
//                    current = current.callForCollection(new Iterative(srcNo, i+"", config, runConfig.getIterative_TempFolder()));
//                    break;
//                case INITIAL_DEPTH:
                case ID:
                    current = current.callForCollection(new NDepthReclustering(srcNo,runConfig.getInitialDepth_IntDepth(),i+"",config));

            }

            srcNo++;
            if (!vertexGroupingKey.equals(""))
                current = current.callForCollection(new Summarize(vertexGroupingKey, wccPropertyKeyList, isRepetitionAllowed, true));
//            else
                current = current.callForCollection(new ClearGraphIds(GradoopId.get()));


            outputPath = tempOutPath+"clustering"+i+"/";
                dataSink = new CSVDataSink(outputPath , current.getConfig());
            dataSink.write(current, true);


            env.execute(resOutInfo+"---"+"inc"+i+"-clustering "+threshold);
            clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");

        }

        FileWriter fw= new FileWriter(resOutPath, true);
        fw.write("info, pre, rec, fm, linkTime, repairTime\n");




            for (int i = 0; i < incrementLength; i++) {

                LogicalGraph evalGraph = null;

                String evalGraphPath = tempOutPath + "clustering" + i + "/";


                    dataSource =
                            new CSVDataSource(evalGraphPath, config);
                    evalGraph = dataSource.getLogicalGraph();


                ComputeClusteringQualityMeasuresWithSameIds1Src clsMes = new
                        ComputeClusteringQualityMeasuresWithSameIds1Src(evalGraph, vertexLabel, false);

                double pre = clsMes.computePrecision();
                double rec = clsMes.computeRecall();
                double fm = clsMes.computeFM();

                fw.write("inc" + i + "_" + resOutInfo + "_" + threshold + "," + pre + "," +
                        rec + "," + fm + "," + linkingRunTimes.split(",")[i] + "*" + clusteringRunTimes.split(",")[i] + "\n");
                fw.flush();
            }

    }


    private static class ClearGraphIds implements UnaryCollectionToCollectionOperator {
        private GradoopId gradoopId;
        public ClearGraphIds (GradoopId gradoopId){
            this.gradoopId = gradoopId;
        }
        @Override
        public GraphCollection execute(GraphCollection input) {

            DataSet<GraphHead> graphHeads = input.getConfig().getExecutionEnvironment().fromElements(new GraphHeadFactory().initGraphHead(gradoopId));
            DataSet<Vertex> vertices = input.getVertices().map(new ResetGraphId(gradoopId));
            DataSet<Edge> edges = input.getEdges().map(new ResetEdgeGraphId(gradoopId));

            return input.getConfig().getGraphCollectionFactory().fromDataSets(graphHeads, vertices, edges);
        }

        @Override
        public String getName() {
            return null;
        }

        private class ResetGraphId implements MapFunction<Vertex, Vertex> {
            private GradoopId gradoopId;
            public ResetGraphId(GradoopId gradoopId) {
                this.gradoopId = gradoopId;
            }

            @Override
            public Vertex map(Vertex vertex) throws Exception {
                GradoopIdSet gradoopIdSet = new GradoopIdSet();
                gradoopIdSet.add(gradoopId);
                vertex.setGraphIds(gradoopIdSet);
                return vertex;
            }
        }
        private class ResetEdgeGraphId implements MapFunction<Edge, Edge> {
            private GradoopId gradoopId;
            public ResetEdgeGraphId(GradoopId gradoopId) {
                this.gradoopId = gradoopId;
            }

            @Override
            public Edge map(Edge edge) throws Exception {
                GradoopIdSet gradoopIdSet = new GradoopIdSet();
                gradoopIdSet.add(gradoopId);
                edge.setGraphIds(gradoopIdSet);
                return edge;
            }
        }
    }
}






























