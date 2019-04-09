package org.tool.system.incremental.incrementalER;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.clustering.parallelClustering.ClusteringGC;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPConfig;
import org.tool.system.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.tool.system.incremental.repairer.methods.AdvancedEarlyMaxBoth;
import org.tool.system.incremental.repairer.methods.AdvancedLateMaxBoth;
import org.tool.system.incremental.repairer.methods.NDepthReclustering;
import org.tool.system.incremental.repairer.util.RepairMethod;
import org.tool.system.incremental.representator.Summarize;
import org.tool.system.incremental.temp.RunConfig;
import org.tool.system.inputConfig.LinkerConfig;
import org.tool.system.linking.linking.Linker;
import org.tool.system.linking.linking.data_structures.LinkerComponent;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EntityIncrementalER {
    public void execute(String confFilePath, int runId, double threshold, String dataset) throws Exception {
        RunConfig runConfig = new RunConfig().readConfig(confFilePath,  runId);

        Double aggThreshold = threshold;

        String linkingConfFilePath = runConfig.getLinkingConfFilePath();
        int incrementLength = runConfig.getIncrementLength();
        String initialCollectionPath = runConfig.getInitialCollectionPath();
        Integer srcNo = runConfig.getSrcNo();
        String tempOutPath = runConfig.getTempOutPath();
        RepairMethod repairMethod = runConfig.getRepairMethod();
        String resOutPath = runConfig.getResOutPath();
        String vertexLabel = runConfig.getVertexLabel();
        String resOutInfo = runConfig.getResOutInfo();
        ////

        String linkingRunTimes = "";
        String clusteringRunTimes = "";
        String inputPath = "";


        String vertexGroupingKey = runConfig.getVertexGroupingKey();
        String[] wccPropertyKeyList = runConfig.getWccPropertyKeyList().split(",");

        Boolean[] isRepetitionAllowed = runConfig.getRepetitionAllowed();
        boolean isEdgeAggregation = runConfig.getIsEdgeAggregation();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        LinkerConfig linkerConfig = new LinkerConfig(aggThreshold);

        List<LinkerComponent> linkerComponents = linkerConfig.readConfig(linkingConfFilePath);


        inputPath = initialCollectionPath+"0/";
        GraphCollection srcGC;

            CSVDataSource src = new CSVDataSource(inputPath, config);
            srcGC = src.getGraphCollection();



//Iteration 1


        GraphCollection current  = srcGC;


        /* Step : linking  */
        LinkerComponent linkerComponent = linkerComponents.get(0);
        Linker linker = new Linker(linkerComponent, dataset);
        current = current.callForCollection(linker);


        String outputPath = tempOutPath+"linking0/";

            CSVDataSink dataSink = new CSVDataSink(outputPath , current.getConfig());
            dataSink.write(current, true);

        env.execute(resOutInfo+"---"+"inc0-linking");
        linkingRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");


            CSVDataSource dataSource =
                    new CSVDataSource(outputPath , config);
            current = dataSource.getGraphCollection();




        /* Step 4: clustering  */
        CLIPConfig clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(srcNo);
        current = current.callForCollection(new ClusteringGC(clipConfig));


        if (!repairMethod.equals(RepairMethod.ID))
            current = current.getConfig().getGraphCollectionFactory().fromDataSets(current.getGraphHeads(), current.getVertices());

        if (!vertexGroupingKey.equals(""))
            current = current.callForCollection(new Summarize(vertexGroupingKey, wccPropertyKeyList, isRepetitionAllowed, isEdgeAggregation));
        else
            current = current.callForCollection(new ClearGraphIds(config.getVertexFactory().createVertex().getId()));


        outputPath = tempOutPath+"clustering0/";

            dataSink = new CSVDataSink(outputPath , current.getConfig());
            dataSink.write(current, true);
            env.execute(resOutInfo+"---"+"inc0-clustering");

        clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");
        ////////////////////////////////////////////////////////////////////////
//Iteration 2+
        for(int i=1; i< incrementLength;i++) {


                dataSource =
                        new CSVDataSource(outputPath, config);
                current = dataSource.getGraphCollection();



        /* Step 1: increment (get vertices) */
            inputPath = initialCollectionPath + i + "/";


                dataSource = new CSVDataSource(inputPath, config);
                srcGC = src.getGraphCollection();



        /* Step 2: add the the next graph  */
            current = current.union(srcGC);

        /* Step 3: linking  */

            linkerComponent = linkerComponents.get(1);
            linker = new Linker(linkerComponent, dataset);
            current = current.callForCollection(linker);


            outputPath = tempOutPath + "linking" + i + "/";

            dataSink = new CSVDataSink(outputPath, current.getConfig());
            dataSink.write(current, true);

            env.execute(resOutInfo+"---"+"inc"+i+"-linking");
            linkingRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");



                dataSource =
                        new CSVDataSource(outputPath, config);
                current = dataSource.getGraphCollection();



        /* Step 4: repairing  */
            switch (repairMethod){
                case EMB:
//                    current = current.callForCollection(new EarlyMaxBoth(srcNo,i+"", Boolean.parseBoolean(args[10])));
                    current = current.callForCollection(new AdvancedEarlyMaxBoth(srcNo,i+"", runConfig.getIsPreclustering(), runConfig.getIsSrcConsistnt()));
                    break;
                case LMB:
                    current = current.callForCollection(new AdvancedLateMaxBoth(srcNo,i+"", runConfig.getIsPreclustering()));
                    break;
                case ITERATIVE:
//                case IT:
//                    current = current.callForCollection(new Iterative(srcNo, i+"", config, runConfig.getIterative_TempFolder()));
//                    break;
                case INITIAL_DEPTH:
                case ID:
                    current = current.callForCollection(new NDepthReclustering(srcNo,runConfig.getInitialDepth_IntDepth(),i+"",config));
            }



            if (!vertexGroupingKey.equals(""))
                current = current.callForCollection(new Summarize(vertexGroupingKey, wccPropertyKeyList,isRepetitionAllowed, isEdgeAggregation));
            else
                current = current.callForCollection(new ClearGraphIds(config.getVertexFactory().createVertex().getId()));


                outputPath = tempOutPath+"clustering"+i+"/";
                dataSink = new CSVDataSink(outputPath, current.getConfig());
                dataSink.write(current, true);

            env.execute(resOutInfo+"---"+"inc"+i+"-clustering");
            clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");
        }

//
        FileWriter fw = new FileWriter(resOutPath, true);
        fw.write("info, pre, rec, fm, linkTime, repairTime\n");

            for (int i=0; i<incrementLength; i++) {

                String evalGraphPath = tempOutPath+"clustering"+i+"/";
                GraphCollection evalGraph = null;

                    dataSource =
                            new CSVDataSource(evalGraphPath , config);
                    evalGraph = dataSource.getGraphCollection();


                ComputeClusteringQualityMeasuresWithSameIds1Src measures = new
                        ComputeClusteringQualityMeasuresWithSameIds1Src(evalGraph, vertexLabel, false);

                fw.write("inc"+i+"_" + resOutInfo+threshold+ "," + measures.computePrecision() + "," +
                        measures.computeRecall() + "," + measures.computeFM() + "," + linkingRunTimes.split(",")[i]+ "*"+clusteringRunTimes.split(",")[i]+ "\n");
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
            DataSet<Vertex> vertices = input.getVertices().map(new ClearGraphIds.ResetGraphId(gradoopId));
            return input.getConfig().getGraphCollectionFactory().fromDataSets(graphHeads, vertices, input.getEdges());
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
    }
}





























