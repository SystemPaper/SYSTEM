package org.tool.system.incremental.incrementalER;

import org.apache.flink.api.java.ExecutionEnvironment;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

public class EntityBatchER {
    public void execute(String confFilePath, int runId, Double threshold, String dataset) throws Exception {

        RunConfig runConfig = new RunConfig().readConfig(confFilePath,  runId);

        Double aggThreshold = threshold;//runConfig.getAggThreshold();
        String linkingConfFilePath = runConfig.getLinkingConfFilePath();
        int incrementLength = runConfig.getIncrementLength();
        String initialCollectionPath = runConfig.getInitialCollectionPath();
        Integer srcNo = runConfig.getSrcNo();
        String tempOutPath = runConfig.getTempOutPath();
        String resOutPath = runConfig.getResOutPath();
        String vertexLabel = runConfig.getVertexLabel();
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

        GraphCollection current  = srcGC;


        /* Step: linking  */
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
        clipConfig.setSourceNo(srcNo);
        current = current.callForCollection(new ClusteringGC(clipConfig));





        outputPath = tempOutPath+"clustering0/";

            csvSink =
                    new CSVDataSink(outputPath , config);
            csvSink.write(current, true);

        env.execute(resOutInfo+"---"+"inc0-clustering");
        clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");

        ////////////////////////////////////////////////////////////////////////
//Iteration 2+
        for(int i=1; i< incrementLength;i++) {


                 csvSource =
                        new CSVDataSource(outputPath , config);
                current = csvSource.getGraphCollection();


        /* Step 1: increment (get vertices) */
            inputPath = initialCollectionPath+i+"/";



                 src = new CSVDataSource(inputPath, config);
                srcGC = src.getGraphCollection();


        /* Step 2: add the the next graph  */

            current = current.union(srcGC);

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


                 src = new CSVDataSource(outputPath, config);
                current = src.getGraphCollection();




        /* Step 4: clustering  */
            clipConfig = new CLIPConfig();
            clipConfig.setSourceNo(srcNo);
            current = current.callForCollection(new ClusteringGC(clipConfig));




            outputPath = tempOutPath+"clustering"+i+"/";

                 csvSink =
                        new CSVDataSink(outputPath , config);
                csvSink.write(current, true);

            env.execute(resOutInfo+"---inc"+i+"-clustering");
            clusteringRunTimes += (env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS)+",");
        }


        FileWriter fw = new FileWriter(resOutPath, true);
        fw.write("info, pre, rec, fm, linkTime, clsTime\n");


            for (int i=0; i<incrementLength; i++) {

                LogicalGraph evalGraph = null;
                String evalGraphPath = tempOutPath+"clustering"+i+"/";


                    CSVDataSource csvDataSource =
                            new CSVDataSource(evalGraphPath , config);
                    evalGraph = csvDataSource.getLogicalGraph();


                ComputeClusteringQualityMeasuresWithSameIds1Src measures = new
                        ComputeClusteringQualityMeasuresWithSameIds1Src(evalGraph, vertexLabel, false);

                fw.write("inc"+i+"_"+runConfig.getId() + ","+threshold+"," + runConfig.getRunId() + "," + resOutInfo + "," + measures.computePrecision() + "," +
                        measures.computeRecall() + "," + measures.computeFM() + "," + linkingRunTimes.split(",")[i]+ "*"+clusteringRunTimes.split(",")[i]+ "\n");
                fw.flush();
            }


    }



}





























