package org.tool.system.incremental.incrementalER;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.clustering.parallelClustering.ClusteringGC;
import org.tool.system.clustering.parallelClustering.util.clip.util.CLIPConfig;
import org.tool.system.common.Quality.ClusteredGraph.ComputeClusteringQualityMeasuresWithSameIds1Src;
import org.tool.system.incremental.temp.RunConfig;
import org.tool.system.inputConfig.LinkerConfig;
import org.tool.system.linking.linking.Linker;
import org.tool.system.linking.linking.data_structures.LinkerComponent;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SourceBatchER {
    public void execute(String confFilePath, int runId, double threshold, String dataset) throws Exception {
//        Logger.getRootLogger().setLevel(Level.DEBUG);
        RunConfig runConfig = new RunConfig().readConfig(confFilePath,  runId);

        Double aggThreshold = threshold;//runConfig.getAggThreshold();
        String linkingConfFilePath = runConfig.getLinkingConfFilePath();
        int incrementLength = runConfig.getIncrementLength();
        String initialCollectionPath = runConfig.getInitialCollectionPath();
        String tempOutPath = runConfig.getTempOutPath();
        String resOutPath = runConfig.getResOutPath();
        String vertexLabel = runConfig.getVertexLabel();
        String allowedGIds = runConfig.getAllowedGraphIds();
        String resOutInfo = runConfig.getResOutInfo();

        String linkingRunTimes = "";
        String clusteringRunTimes = "";

        String inputPath = "";


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        LinkerConfig linkerConfig = new LinkerConfig(aggThreshold);

        List<LinkerComponent> linkerComponents = linkerConfig.readConfig(linkingConfFilePath);


        inputPath = initialCollectionPath+"0/";

        GraphCollection srcGC = null;

        CSVDataSource src = new CSVDataSource(inputPath, config);
        srcGC = src.getGraphCollection();

        DataSet<Vertex> incrementedVertices = srcGC.getVertices();


        LogicalGraph graph = config.getLogicalGraphFactory().fromDataSets(incrementedVertices);
        GraphCollection current = config.getGraphCollectionFactory().fromGraph(graph);


        inputPath = initialCollectionPath+"1/";

        src = new CSVDataSource(inputPath, config);
        srcGC = src.getGraphCollection();

        incrementedVertices = srcGC.getVertices();


        graph = config.getLogicalGraphFactory().fromDataSets(incrementedVertices);
        GraphCollection graphCollection = config.getGraphCollectionFactory().fromGraph(graph);
        current = current.union(graphCollection);

        LinkerComponent linkerComponent = linkerComponents.get(0);
        Linker linker = new Linker(linkerComponent, dataset);
        current = current.callForCollection(linker);

        String outputPath = tempOutPath+"linking0/";

        CSVDataSink csvSink =
                    new CSVDataSink(outputPath , config);
        csvSink.write(current, true);



        env.execute(resOutInfo+"---"+"inc0-linking");
        linkingRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");




        CSVDataSource csvSource =
                    new CSVDataSource(outputPath , config);
        current = csvSource.getGraphCollection();


        /* Step 4: clustering  */
        CLIPConfig clipConfig = new CLIPConfig();
        clipConfig.setSourceNo(2);
        current = current.callForCollection(new ClusteringGC(clipConfig));

//////////////////////////////
        DataSet<Vertex> vertices = current.getVertices();
        vertices = vertices.map(new CleanIds(allowedGIds));
        current = current.getConfig().getGraphCollectionFactory().fromDataSets(current.getGraphHeads(),vertices);
/////////////////////////////


        outputPath = tempOutPath+"clustering0/";

            csvSink =
                    new CSVDataSink(outputPath , config);
            csvSink.write(current, true);

        env.execute(resOutInfo+"---"+"inc0-clustering");
        clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");

        ////////////////////////////////////////////////////////////////////////
//Iteration 2+
        int srcNo = 3;
        incrementLength--;
        for(int i=1; i< incrementLength;i++) {


                csvSource =
                        new CSVDataSource(outputPath , config);
                current = csvSource.getGraphCollection();



        /* Step 1: increment (get vertices) */
            inputPath = initialCollectionPath+(i+1)+"/";


                src = new CSVDataSource(inputPath, config);
                srcGC = src.getGraphCollection();

            incrementedVertices = srcGC.getVertices();
        /* Step 2: add the the next graph  */
            graphCollection = config.getGraphCollectionFactory().fromDataSets(srcGC.getGraphHeads(),incrementedVertices);

            current = current.union(graphCollection);

        /* Step 3: linking  */

            linkerComponent = linkerComponents.get(0);
            linker = new Linker(linkerComponent, dataset);
            current = current.callForCollection(linker);


            outputPath = tempOutPath+"linking"+i+"/";

                csvSink =
                        new CSVDataSink(outputPath , config);
                csvSink.write(current, true);

            env.execute(resOutInfo+"---"+"inc"+i+"-linking");
            linkingRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");


                csvSource =
                        new CSVDataSource(outputPath , config);
                current = csvSource.getGraphCollection();



        /* Step 4: clustering  */
            clipConfig = new CLIPConfig();
            clipConfig.setSourceNo(srcNo);
            current = current.callForCollection(new ClusteringGC(clipConfig));

//////////////////////////////
            vertices = current.getVertices();
            vertices = vertices.map(new CleanIds(allowedGIds));
            current = current.getConfig().getGraphCollectionFactory().fromDataSets(current.getGraphHeads(),vertices);
/////////////////////////////


            outputPath = tempOutPath+"clustering"+i+"/";

                csvSink =
                        new CSVDataSink(outputPath , config);
                csvSink.write(current, true);

            env.execute(resOutInfo+"---inc"+i+"-clustering");
            clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");

            srcNo++;
        }
//
        FileWriter fw = new FileWriter(resOutPath, true);
        fw.write("info, pre, rec, fm, linkTime, clsTime\n");


            for (int i=0; i<incrementLength; i++) {

                String evalGraphPath = tempOutPath+"clustering"+i+"/";
                GraphCollection evalGraph = null;

                    csvSource =
                            new CSVDataSource(evalGraphPath , config);
                    evalGraph = csvSource.getGraphCollection();


                ComputeClusteringQualityMeasuresWithSameIds1Src measures = new
                        ComputeClusteringQualityMeasuresWithSameIds1Src(evalGraph, vertexLabel, false);

                fw.write("inc"+i+"_"+runConfig.getId() + "," + runConfig.getRunId() + "," + resOutInfo + "," + measures.computePrecision() + "," +
                        measures.computeRecall() + "," + measures.computeFM() + "," + linkingRunTimes.split(",")[i]+ "*"+clusteringRunTimes.split(",")[i]+ "\n");
                fw.flush();
            }


    }

    private class CleanIds implements MapFunction<Vertex, Vertex> {
        private List<GradoopId> allowed;
        public CleanIds (String allowedGIds){
            allowed = new ArrayList<>();
            String[] allowedGIdList = allowedGIds.split(",");
            for(String id:allowedGIdList){
                allowed.add(new GradoopId().fromString(id));
            }

        }
        @Override
        public Vertex map(Vertex vertex) throws Exception {

            GradoopIdSet curGraphIds = vertex.getGraphIds();
            GradoopIdSet newGraphIds = new GradoopIdSet();


            for (GradoopId id:curGraphIds){
                if (allowed.contains(id))
                    newGraphIds.add(id);
            }
            vertex.setGraphIds(newGraphIds);
            return vertex;
        }
    }


}





























