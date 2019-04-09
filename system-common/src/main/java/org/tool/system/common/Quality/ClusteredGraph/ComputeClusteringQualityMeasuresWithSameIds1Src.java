package org.tool.system.common.Quality.ClusteredGraph;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.tool.system.common.Quality.ClusteredGraph.functions.*;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.count.Count;

import java.io.Serializable;
import java.util.List;

/** It is used when there is no Golden Truth file and noly vertices with the same ids are true positives
 * Computes Precision, Recall, and FMeasue of the input
 * The input can be a GraphCollection or a LogicalGraph.
 *
 */
public class ComputeClusteringQualityMeasuresWithSameIds1Src {
//    private static String GTPath;
//    private static String GTSplitter;
    private GraphCollection inputGrphCltion;
    private LogicalGraph inputGrph;
    private static DataSet<Tuple2<String, Long>> tpset;
    private static DataSet<Tuple2<String, Long>> apset;
    private static DataSet<Tuple2<String, Long>> gtRecorsNoSet;
    private static DataSet<Tuple2<String, Long>> clusterNoSet;
    private static DataSet<Tuple2<String, Long>> repAPSet;
    private static DataSet<Tuple2<String, Long>> repTPSet;
    private static DataSet<Tuple2<String, Long>> clusterSizeSet;
    private static DataSet<Tuple2<String, Long>> minClusterSizeSet;
    private static DataSet<Tuple2<String, Long>> maxClusterSizeSet;
    private static DataSet<Tuple2<String, Long>> singleSet;
    private static DataSet<Tuple2<String, Long>> perfectClusterNoSet;
    private static DataSet<Tuple2<String, Long>> perfectCompleteClusterNoSet;

    private static DataSet<Tuple2<String, Long>> phase1VertexNoSet;
    private static DataSet<Tuple2<String, Long>> phase2VertexNoSet;
    private static DataSet<Tuple2<String, Long>> phase3VertexNoSet;

    private int srcNo;


    private String vertexIdLabel;
    private static boolean isGTFull;
    private static ExecutionEnvironment env;
    private static Long tp;
    private static Long ap;
    private static Long gtRecorsNo;
    private static Long clusterNo;
    private static Long repPositiveNo;
    private static Long repTruePositiveNo;
    private static boolean isWithinDataSetMatch;
    private static boolean hasOverlap;
    private Double aveClusterSize;
    private Long minClusterSize;
    private Long maxClusterSize;
    private Long clusterSize;
    private Long single;
    private Long perfect;
    private Long perfectComplete;
    private Long phase1VertexNo;
    private Long phase2VertexNo;
    private Long phase3VertexNo;






    public ComputeClusteringQualityMeasuresWithSameIds1Src(GraphCollection ClusteringResult, String VertexIdLabel, boolean HasOverlap) {
        inputGrphCltion = ClusteringResult;
        vertexIdLabel = VertexIdLabel;
        tpset = apset = gtRecorsNoSet = clusterNoSet = null;
        ap = tp = gtRecorsNo = clusterNo = single = minClusterSize = maxClusterSize =  -1L;
        aveClusterSize=-1d;
        inputGrph = null;
        hasOverlap = HasOverlap;
        env = ClusteringResult.getConfig().getExecutionEnvironment();
        repAPSet = env.fromElements(Tuple2.of("repAP",0l));
        repTPSet = env.fromElements(Tuple2.of("repTP",0l));
        perfectClusterNoSet = perfectCompleteClusterNoSet = null;
        srcNo = 5;
        perfect = perfectComplete=-1l;
        phase1VertexNo = phase2VertexNo = phase3VertexNo = -1l;
    }
    public ComputeClusteringQualityMeasuresWithSameIds1Src(LogicalGraph ClusteringResult, String VertexIdLabel, boolean HasOverlap) {
        aveClusterSize=-1d;
        inputGrphCltion = null;
        vertexIdLabel = VertexIdLabel;
       tpset = apset = gtRecorsNoSet = clusterNoSet = null;
        ap = tp = gtRecorsNo = clusterNo = single = minClusterSize = maxClusterSize =-1L;
        inputGrph = ClusteringResult;
        hasOverlap = HasOverlap;
        env = ClusteringResult.getConfig().getExecutionEnvironment();
        repAPSet = env.fromElements(Tuple2.of("repAP",0l));
        repTPSet = env.fromElements(Tuple2.of("repTP",0l));
        perfectClusterNoSet = perfectCompleteClusterNoSet = null;
        srcNo = 5;
        perfect = perfectComplete=-1l;
        phase1VertexNo = phase2VertexNo = phase3VertexNo = -1l;
    }
    public ComputeClusteringQualityMeasuresWithSameIds1Src(LogicalGraph ClusteringResult, String VertexIdLabel, boolean HasOverlap, Integer inputSrcNo) {
        aveClusterSize=-1d;
        inputGrphCltion = null;
        vertexIdLabel = VertexIdLabel;
        tpset = apset = gtRecorsNoSet = clusterNoSet = null;
        ap = tp = gtRecorsNo = clusterNo = single = minClusterSize = maxClusterSize =-1L;
        inputGrph = ClusteringResult;
        hasOverlap = HasOverlap;
        env = ClusteringResult.getConfig().getExecutionEnvironment();
        repAPSet = env.fromElements(Tuple2.of("repAP",0l));
        repTPSet = env.fromElements(Tuple2.of("repTP",0l));
        perfectClusterNoSet = perfectCompleteClusterNoSet = null;
        srcNo = inputSrcNo;
        perfect = perfectComplete=-1l;
        phase1VertexNo = phase2VertexNo = phase3VertexNo = -1l;
    }

    public void computeSets() throws Exception {

        DataSet<Vertex> resultVertices = null;
        if (inputGrphCltion != null) {
            resultVertices = inputGrphCltion.getVertices();


        } else {
            resultVertices = inputGrph.getVertices();

        }

//        DataSet<Tuple2<GradoopId, String>> vertexIdPubId = resultVertices.map(new MapFunction<Vertex, Tuple2<GradoopId, String>>() {
//            public Tuple2<GradoopId, String> map(Vertex in) {
////                return Tuple2.of(in.getId(), in.getPropertyValue("clusterId").toString());
////                String recId= in.getPropertyValue("recId").toString().split("s")[0];
//                String recId= in.getPropertyValue(vertexIdLabel).toString();
//                return Tuple2.of(in.getId(), recId);
//
//            }
//        });
        DataSet<Tuple1<String>> recIds = resultVertices.flatMap(new GetRecId(vertexIdLabel));
        gtRecorsNoSet = recIds.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple2<String, Long>>() {
            public void reduce(Iterable<Tuple1<String>> values, Collector<Tuple2<String, Long>> out) throws Exception {
                Long cnt = 0l;
                for (Tuple1<String> v:values) {
                    cnt++;
                }

                out.collect(Tuple2.of("gt",(cnt*(cnt-1)/2)));
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of("gt",value1.f1+value2.f1);
            }
        });
        if(hasOverlap){
            DataSet<Tuple3<String, String, String>> vertexId_pubId_ClusterId = resultVertices.flatMap(new FlatMapFunction<Vertex, Tuple3<String, String, String>>() {
                @Override
                public void flatMap(Vertex vertex, Collector<Tuple3<String, String, String>> out) throws Exception {
                    if (vertex.getPropertyValue("ClusterId").toString().contains(",")){
                        String[] clusterIds = vertex.getPropertyValue("ClusterId").toString().split(",");
//                        String recId = vertex.getPropertyValue("recId").toString().split("s")[0];
                        String recId= vertex.getPropertyValue(vertexIdLabel).toString();
                        for (int i=0; i <clusterIds.length; i++) {
//                            out.collect(Tuple3.of(vertex.getId().toString(),vertex.getPropertyValue("clusterId").toString(), clusterIds[i]));
                            out.collect(Tuple3.of(vertex.getId().toString(), recId, clusterIds[i]));
                        }

                    }
                }
            });

            DataSet<Tuple2<String, String>> pairedVertices__pubId1pubId2 = vertexId_pubId_ClusterId.join(vertexId_pubId_ClusterId).where(2).equalTo(2).with(new JoinFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple6<String, String,String, String, String, String>>() {
                public Tuple6<String, String, String, String,String, String> join(Tuple3<String, String, String> in1, Tuple3<String,String, String> in2) {
//                    if(in1.f0.compareTo(in2.f0) < 0) {
                        return Tuple6.of(in1.f0, in1.f1, in1.f2,in2.f0, in2.f1, in2.f2);
//                    }
//                    else {
//                        return Tuple6.of(in2.f0, in2.f1, in2.f2,in1.f0, in1.f1, in1.f2);
//                    }
                }
            }).flatMap(new FlatMapFunction<Tuple6<String, String, String, String,String, String>, Tuple2<String, String>>() {
                public void flatMap(Tuple6<String, String, String, String, String, String> in, Collector<Tuple2<String, String>> out) {
                    if (in.f0.compareTo(in.f3)<0) {
                        out.collect(Tuple2.of(in.f0 + "," + in.f3, in.f1+","+ in.f4 ));
                    }
                }
            });
            repAPSet =  pairedVertices__pubId1pubId2.groupBy(0).reduceGroup(new ComputeCountbyReduce()).reduce(new reduceLongtoSet()).map(new LongtoSet("repAP"));
            pairedVertices__pubId1pubId2 =  pairedVertices__pubId1pubId2.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                @Override
                public void flatMap(Tuple2<String, String> in, Collector<Tuple2<String, String>> out) throws Exception {
                    if (in.f1.split(",")[0].equals(in.f1.split(",")[1])) {
//                         System.out.println(in.f0+"*******"+in.f1);

//                        System.out.println(in.f0.split(",")[0]+"*******"+(in.f0.split(",")[1]));
                        out.collect(in);
                    }
                }
            });
            repTPSet = pairedVertices__pubId1pubId2.groupBy(0).reduceGroup(new ComputeCountbyReduce()).reduce(new reduceLongtoSet()).map(new LongtoSet("repTP"));
            resultVertices = resultVertices.flatMap(new FlatMapFunction<Vertex, Vertex>() {
                public void flatMap(Vertex vertex, Collector<Vertex> collector) throws Exception {
                    String clusterids = vertex.getPropertyValue("ClusterId").toString();
                    if (clusterids.contains(",")) {
                        String[] clusteridsArray = clusterids.split(",");
                        for (int i=0;i<clusteridsArray.length;i++) {
                            vertex.setProperty("ClusterId",clusteridsArray[i]);
                            collector.collect(vertex);
                        }
                    }
                    else
                        collector.collect(vertex);
                }
            });

        } // if has overlap


        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//        DataSet<Tuple3<String, String, String>> verticesClusterIdsTypes = resultVertices.flatMap(new FlatMapFunction<Vertex, Tuple3<String, String, String>>() {
//            @Override
//            public void flatMap(Vertex in, Collector<Tuple3<String, String, String>> out) throws Exception {
////                String recId= in.getPropertyValue("recId").toString().split("s")[0];
//
//                String recId = in.getPropertyValue(vertexIdLabel).toString();
//
//                out.collect(Tuple3.of(recId,
//                        in.getPropertyValue("ClusterId").toString(), in.getPropertyValue("type").toString()));
//            }
//        });
        DataSet<Tuple3<String, String, String>> verticesClusterIdsTypes = resultVertices.flatMap(new GetVClsIdTypes(vertexIdLabel));

        DataSet <Tuple2 <Long,Long>> p_pc = verticesClusterIdsTypes.groupBy(1).reduceGroup(new ppcReducer(srcNo));
        perfectClusterNoSet = p_pc.reduce(new reduceTuple2toSet(0)).map(new Tuple2toSet("p",0));
        perfectCompleteClusterNoSet = p_pc.reduce(new reduceTuple2toSet(1)).map(new Tuple2toSet("pc",1));


        DataSet<Integer> phaseDistribution = verticesClusterIdsTypes.map(new MapFunction<Tuple3<String, String, String>, Integer>() {
            @Override
            public Integer map(Tuple3<String, String, String> in) throws Exception {
                if (in.f1.split("-")[0].contains("1"))
                    return 1;
                if (in.f1.split("-")[0].contains("2"))
                    return 2;
                if (in.f1.split("-")[0].contains("3"))
                    return 3;
                return 0;
            }
        });
        DataSet<Integer> phase1 = phaseDistribution.flatMap(new getSpecificPhase(1));
        DataSet<Integer> phase2 = phaseDistribution.flatMap(new getSpecificPhase(2));
        DataSet<Integer> phase3 = phaseDistribution.flatMap(new getSpecificPhase(3));

        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

                //////All Positives
//                DataSet < Tuple1 < String >> ClusterIds = resultVertices.flatMap(new MyVertex2Vertex_ClusterId(true)).map(new GetF1Tuple2()).map(new MapFunction<String, Tuple1<String>>() {
//                    @Override
//                    public Tuple1<String> map(String value) throws Exception {
//                        return Tuple1.of(value);
//                    }
//                });
        DataSet < Tuple1 < String >> ClusterIds = resultVertices.flatMap(new MyVertex2Vertex_ClusterId());
        DataSet<Tuple2<Long, Long>> clstrSize_apNo =  ClusterIds.groupBy(0).reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple2<Long, Long>>() {
                    @Override
                    public void reduce(Iterable<Tuple1<String>> in, Collector<Tuple2<Long, Long>> out) throws Exception {
                        long cnt=0l;
                        for (Tuple1<String> i:in)
                            cnt++;
                        out.collect(Tuple2.of(cnt,cnt*(cnt-1)/2));
                    }
                });
        singleSet = clstrSize_apNo.filter(new FilterFunction<Tuple2<Long, Long>>() {
            @Override
            public boolean filter(Tuple2<Long, Long> value) throws Exception {
                return value.f0==1;
            }
        })
        .reduce(new reduceTuple2toSet(0)).map(new Tuple2toSet("single",0));
        apset= clstrSize_apNo.reduce(new reduceTuple2toSet(1)).map(new Tuple2toSet("ap",1));
        clusterSizeSet = clstrSize_apNo.reduce(new reduceTuple2toSet(0)).map(new Tuple2toSet("cs",0));
        maxClusterSizeSet = clstrSize_apNo.aggregate(Aggregations.MAX, 0).map(new Tuple2toSet("maxcs",0));
        minClusterSizeSet = clstrSize_apNo.aggregate(Aggregations.MIN, 0).map(new Tuple2toSet("mincs",0));
        clusterNoSet = Count.count(clstrSize_apNo).map(new LongtoSet("cn"));
        phase1VertexNoSet = Count.count(phase1).map(new LongtoSet("ph1"));
        phase2VertexNoSet = Count.count(phase2).map(new LongtoSet("ph2"));
        phase3VertexNoSet = Count.count(phase3).map(new LongtoSet("ph3"));


        ///////True Positives
//        DataSet<Tuple1<String>> ClusterIdPubId = resultVertices.map(new MapFunction<Vertex, Tuple1<String>>() {
//            public Tuple1<String> map(Vertex in) {
////                return Tuple1.of(in.getPropertyValue("ClusterId").toString()+","+in.getPropertyValue("clusterId").toString());
////                String recId= in.getPropertyValue("recId").toString().split("s")[0];
//                String recId= in.getPropertyValue(vertexIdLabel).toString();
//
//                return Tuple1.of(in.getPropertyValue("ClusterId").toString()+","+recId);
//
//            }
//        });
        DataSet<Tuple1<String>> ClusterIdPubId = resultVertices.flatMap(new ConcatClsIdRecId(vertexIdLabel));
        tpset = ClusterIdPubId.groupBy(0).
                reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple2<String, Long>>() {
            @Override
            public void reduce(Iterable<Tuple1<String>> in, Collector<Tuple2<String, Long>> out) throws Exception {
                long cnt = 0l;
                for (Tuple1<String> i:in)
                    cnt++;
                out.collect(Tuple2.of("tp",cnt*(cnt-1)/2));
            }
        }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> in1, Tuple2<String, Long> in2) throws Exception {
                return Tuple2.of("tp", in1.f1+in2.f1);
            }
        });


    }


    private void computeValues() throws Exception {

        if (clusterNoSet == null)
            computeSets();
        DataSet<Tuple2<String, Long>> sets = tpset.union(apset).union(gtRecorsNoSet).union(repAPSet).union(repTPSet).union(maxClusterSizeSet)
                .union(minClusterSizeSet).union(clusterNoSet).union(clusterSizeSet).union(singleSet).union(perfectClusterNoSet).union(perfectCompleteClusterNoSet)
                .union(phase1VertexNoSet).union(phase2VertexNoSet).union(phase3VertexNoSet);
        repPositiveNo=repTruePositiveNo=0l;
        for (Tuple2<String, Long> i : sets.collect()) {
            if (i.f0.equals("ap")) {
                ap = i.f1;
//                System.out.println("ap "+ap);

            }
            else if (i.f0.equals("tp")) {
                tp = i.f1;
//                System.out.println("tp "+tp);

            }
            else if (i.f0.equals("gt")) {
                gtRecorsNo = i.f1;
//                System.out.println("gtRecorsNo "+gtRecorsNo);
            }
            else if (i.f0.equals("repAP")) {
                repPositiveNo = i.f1;
//                System.out.println("repPositiveNo: "+repPositiveNo);
            }
            else if (i.f0.equals("repTP")) {
                repTruePositiveNo = i.f1;
//                System.out.println("repTruePositiveNo: "+repTruePositiveNo);
            }
            else if (i.f0.equals("maxcs")) {
                maxClusterSize = i.f1;
//                System.out.println("maxClusterSize: "+maxClusterSize);
            }
            else if (i.f0.equals("mincs")) {
                minClusterSize = i.f1;
//                System.out.println("minClusterSize: "+minClusterSize);
            }
            else if (i.f0.equals("cs")) {
                clusterSize = i.f1;
//                System.out.println("clusterSize: "+clusterSize);
            }
            else if (i.f0.equals("cn")) {
                clusterNo = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
            else if (i.f0.equals("single")) {
                single = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
            else if (i.f0.equals("p")) {
                perfect = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
            else if (i.f0.equals("pc")) {
                perfectComplete = i.f1;
//                System.out.println("clusterNo: "+clusterNo);
            }
            else if (i.f0.equals("ph1")) {
                phase1VertexNo = i.f1;
            }
            else if (i.f0.equals("ph2")) {
                phase2VertexNo = i.f1;
            }
            else if (i.f0.equals("ph3")) {
                phase3VertexNo = i.f1;
            }
        }
        aveClusterSize = (double)clusterSize/clusterNo;

        if (hasOverlap){
//            System.out.println(ap+"********eval2**********"+repPositiveNo);
//            System.out.println(tp+"**********eval2********"+repTruePositiveNo);
            ap -= repPositiveNo;
            tp -= repTruePositiveNo;
        }
    }



    public long getTP() throws Exception {
        if (tp == -1)
            computeValues();
        return tp;
    }


    public long getAP() throws Exception {
        if (ap == -1)
            computeValues();
        return ap;
    }

    public long getGtRecordsNo() throws Exception {
        if (gtRecorsNo == -1)
            computeValues();

        return gtRecorsNo;
    }

    public long getClusterNo() throws Exception {
        if (clusterNo == -1)
            computeValues();
        return clusterNo;
    }

    public Double computePrecision() throws Exception {
        if (tp == -1)
            computeValues();
        return (double) tp / ap;
    }

    public Double computeRecall() throws Exception {
        if (tp == -1)
            computeValues();
        return (double) tp / gtRecorsNo;
    }

    public Double computeFM() throws Exception {
        if (tp == -1)
            computeValues();
        double pr = computePrecision();
        double re = computeRecall();
        return 2 * pr * re / (pr + re);
    }
    public long getMinClsterSize() throws Exception {
        if (minClusterSize == -1)
            computeValues();
        return minClusterSize;
    }
    public long getMaxClsterSize() throws Exception {
        if (maxClusterSize == -1)
            computeValues();
        return maxClusterSize;
    }
    public long getSumClsterSize() throws Exception {
        if (clusterSize == -1)
            computeValues();
        return clusterSize;
    }
    public double getAveClsterSize() throws Exception {
        if (aveClusterSize == -1)
            computeValues();
        return aveClusterSize;
    }
    public double getSingletons() throws Exception {
        if (single == -1)
            computeValues();
        return single;
    }
    public Long getPerfectClusterNo()throws Exception{
        if (perfect == -1)
            computeValues();
        return perfect;
    }
    public Long getPerfectCompleteClusterNo() throws Exception{
        if (perfectComplete == -1)
            computeValues();
        return perfectComplete;
    }
    public Long getPhase1ClusterdVertices() throws Exception{
        if (phase1VertexNo == -1)
            computeValues();
        return phase1VertexNo;
    }
    public Long getPhase2ClusterdVertices() throws Exception{
        if (phase2VertexNo == -1)
            computeValues();
        return phase2VertexNo;
    }
    public Long getPhase3ClusterdVertices() throws Exception{
        if (phase3VertexNo == -1)
            computeValues();
        return phase3VertexNo;
    }

    private class GetRecId implements Serializable, FlatMapFunction<Vertex, Tuple1<String>> {
        private String idLabel;
        public GetRecId (String idLabel){
            this.idLabel = idLabel;
        }
        @Override
        public void flatMap(Vertex vertex, Collector<Tuple1<String>> out) throws Exception {
            if(vertex.hasProperty(idLabel))
               out.collect(Tuple1.of(vertex.getPropertyValue(idLabel).toString()));
            else {
                List<PropertyValue> list = vertex.getPropertyValue("vertices_" + idLabel).getList();
//                if(list.size() == 1){
//                    int cnt =  vertex.getPropertyValue("vertices_graphLabel").getList().size();
//                    String recId = list.get(0).toString();
//                    for (int i=0; i< cnt; i++)
//                        out.collect(Tuple1.of(recId));
//                }
//                else {
                    for (PropertyValue l : list) {
                        out.collect(Tuple1.of(l.toString()));
//                    }
                }
            }
        }
    }
    private class GetVClsIdTypes implements FlatMapFunction <Vertex, Tuple3<String, String, String>>{
        private String idLabel;
        public GetVClsIdTypes (String idLabel){
            this.idLabel = idLabel;
        }
        @Override
        public void flatMap(Vertex in, Collector<Tuple3<String, String, String>> out) throws Exception {
            String recId = "";
            String clusterId = in.getPropertyValue("ClusterId").toString();
            if (in.hasProperty(idLabel)) {
                recId = in.getPropertyValue(idLabel).toString();
                out.collect(Tuple3.of(recId,
                        clusterId, in.getPropertyValue("graphLabel").toString()));
            }
            else {
                List<PropertyValue> recIdList = in.getPropertyValue("vertices_" + idLabel).getList();
                List<PropertyValue> labelList = in.getPropertyValue("vertices_graphLabel").getList();
//                if(recIdList.size()==1) {
//                    for (int i = 0; i < labelList.size(); i++) {
//                        out.collect(Tuple3.of(recIdList.get(0).toString(),
//                                clusterId, labelList.get(i).toString()));
//                    }
//                }
//                else {
                    for (int i = 0; i < labelList.size(); i++) {
                        out.collect(Tuple3.of(recIdList.get(i).toString(),
                                clusterId, labelList.get(i).toString()));
//                    }
                }
            }
        }
    }
    private class ConcatClsIdRecId implements FlatMapFunction<Vertex, Tuple1<String>> {
        private String idLabel;
        public ConcatClsIdRecId (String idLabel){
            this.idLabel = idLabel;
        }
        @Override
        public void flatMap(Vertex in, Collector<Tuple1<String>> out) throws Exception {
            String recId= "";
            String clusterId = in.getPropertyValue("ClusterId").toString();
            if (in.hasProperty(idLabel)) {
                recId = in.getPropertyValue(idLabel).toString();
                out.collect(Tuple1.of(clusterId + "," + recId));
            }
            else {
                List<PropertyValue> list = in.getPropertyValue("vertices_"+idLabel).getList();
//                if(list.size() == 1){
//                    int cnt =  in.getPropertyValue("vertices_graphLabel").getList().size();
//                    recId = list.get(0).toString();
//                    for (int i=0; i< cnt; i++)
//                        out.collect(Tuple1.of(clusterId + "," + recId));
//                }
//                else {
                    for (PropertyValue l : list) {
                        out.collect(Tuple1.of(clusterId + "," + l));
//                    }
                }
            }
        }
    }
    private class MyVertex2Vertex_ClusterId implements FlatMapFunction<Vertex, Tuple1<String>> {

        @Override
        public void flatMap(Vertex in, Collector<Tuple1<String>> out) throws Exception {
            int cnt = 1;
            if (in.hasProperty("vertices_graphLabel"))
                cnt = in.getPropertyValue("vertices_graphLabel").getList().size();
            for (int i=0; i< cnt; i++)
                out.collect(new Tuple1(in.getPropertyValue("ClusterId").toString()));
        }
    }
    public class getSpecificPhase implements FlatMapFunction<Integer, Integer> {
        private Integer phase;
        public getSpecificPhase (Integer inputPhase) {phase = inputPhase;}
        @Override
        public void flatMap(Integer input, Collector<Integer> collector) throws Exception {
            if (input.equals(phase))
                collector.collect(input);
        }
    }


}

