package org.tool.system.common.Quality.InputGraph;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.count.Count;

/**
 * Computes precision, recall, and fmeasure of the input graph
 * It does not consider transitive closure. Number of positives (true positives + false positives) is the number of edges
 */
public class ComputeSimGraphQualityMeasuresforWDI {
    private static String GTPath;
    private static String GTSplitter;
    private LogicalGraph inputGrph;
    private static DataSet<Tuple2<String, Long>> tpset;
    private static DataSet<Tuple2<String, Long>> apset;
    private static DataSet<Tuple2<String, Long>> gtRecorsNoSet;
    private static String vertexIdLabel;

    private static ExecutionEnvironment env;


    private static Long tp;
    private static Long ap;
    private static Long gtRecorsNo;

    public ComputeSimGraphQualityMeasuresforWDI(String groundTruthFilepath, String groundTruthSplitter, LogicalGraph inputGraph, String VertexIdLabel) {
        GTPath = groundTruthFilepath.trim();
        GTSplitter = groundTruthSplitter;
        inputGrph = inputGraph;
        vertexIdLabel = VertexIdLabel;
        tpset = apset = gtRecorsNoSet = null;
        ap = tp = gtRecorsNo = -1L;
        env = inputGraph.getConfig().getExecutionEnvironment();
    }

    private void computeSets() throws Exception {
        DataSet<Tuple2<String, String>> groundTruthFile = env.readTextFile(GTPath).flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            public void flatMap(String line, Collector<Tuple2<String, String>> out) {
                String[] split = line.split(",");
                out.collect(Tuple2.of(split[0].replace("\"", ""), split[1].replace("\"", "")));
            }
        });
//        groundTruthFile= groundTruthFile.map(new MapFunction<Tuple2<String, String>, Tuple3<String, String, String>>() {
//            public Tuple3<String, String, String> map(Tuple2<String, String> value) throws Exception {
//                return Tuple3.of(value.f0, value.f1, value.f0+","+value.f1);
//            }
//        }).groupBy(2).reduceGroup(new GroupReduceFunction<Tuple3<String, String, String>, Tuple2<String, String>>() {
//            public void reduce(Iterable<Tuple3<String, String, String>> values, Collector<Tuple2<String, String>> out) throws Exception {
//                int cnt = 0;
//                for (Tuple3<String, String, String> v:values){
//                    cnt++;
//                    if (cnt>1)
//                    System.out.println(cnt+" "+v.f0+" "+ v.f1);
//                    out.collect(Tuple2.of(v.f0, v.f1));
//                }
//            }
//        });
        DataSet<Tuple2<GradoopId, String>> vertexIdPubId = inputGrph.getVertices().map(new MapFunction<Vertex, Tuple2<GradoopId, String>>() {
            public Tuple2<GradoopId, String> map(Vertex in) {
                return Tuple2.of(in.getId(), in.getPropertyValue("recId").toString());
            }
        });
        DataSet<Tuple2<GradoopId, GradoopId>> sourceIdtargetId = inputGrph.getEdges().map(new MapFunction<Edge, Tuple2<GradoopId, GradoopId>>() {
            public Tuple2<GradoopId, GradoopId> map(Edge in) {
                return Tuple2.of(in.getSourceId(), in.getTargetId());
            }
        });
        DataSet<Tuple2<String, String>> sourcePubIdtargetPubId = vertexIdPubId.join(sourceIdtargetId).where(0).equalTo(0).with
                (new JoinFunction<Tuple2<GradoopId, String>, Tuple2<GradoopId, GradoopId>, Tuple2<String, GradoopId>>() {
                    public Tuple2<String, GradoopId> join(Tuple2<GradoopId, String> in1, Tuple2<GradoopId, GradoopId> in2) {
                        return Tuple2.of(in1.f1, in2.f1);
                    }
                }).join(vertexIdPubId).where(1).equalTo(0).with
                (new JoinFunction<Tuple2<String, GradoopId>, Tuple2<GradoopId, String>, Tuple2<String, String>>() {
                    public Tuple2<String, String> join(Tuple2<String, GradoopId> in1, Tuple2<GradoopId, String> in2) {
                        return Tuple2.of(in1.f0, in2.f1);
                    }
                });


        sourcePubIdtargetPubId = sourcePubIdtargetPubId.map(new MapFunction<Tuple2<String,String>, Tuple1<String>>() {
            public Tuple1<String> map (Tuple2<String,String> in){
                return Tuple1.of(in.f0+","+in.f1);
            }
        }).groupBy(0).reduceGroup(new GroupReduceFunction<Tuple1<String>, Tuple2<String, String>>() {
            public void reduce(Iterable<Tuple1<String>> in, Collector<Tuple2<String, String>> out) throws Exception {
                for (Tuple1<String> i:in){
                    out.collect(Tuple2.of(i.f0.split(",")[0], i.f0.split(",")[1]));
                    break;
                }

            }
        });


        tpset= sourcePubIdtargetPubId.join(groundTruthFile).where(0).equalTo(0).with(
                new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple4<String, String, String, String>>() {
                    public Tuple4<String, String, String, String> join(Tuple2<String, String> in1, Tuple2<String, String> in2) {
                        return Tuple4.of(in1.f0, in1.f1, in2.f0, in2.f1);
                    }
                })
                .flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, Tuple2<String, Long>>() {
                    public void flatMap(Tuple4<String, String, String, String> in, Collector<Tuple2<String, Long>> out) {
                        if (in.f1.equals(in.f3))
                            out.collect(Tuple2.of("tp",1L));
                    }
                }).reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> a, Tuple2<String, Long> b) {
                        return Tuple2.of("tp",a.f1+b.f1);
                    }
                });

        gtRecorsNoSet = Count.count(groundTruthFile).map(new MapFunction<Long, Tuple2<String, Long>>() {
            public Tuple2<String, Long> map(Long a) {
                return Tuple2.of("gt", a);
            }
        });

        apset = Count.count(inputGrph.getEdges()).map(new MapFunction<Long, Tuple2<String, Long>>() {
            public Tuple2<String, Long> map(Long a) {
                return Tuple2.of("ap", a);
            }
        });
    }
    private void computeValues() throws Exception {
        if (apset == null)
            computeSets();
        DataSet<Tuple2<String, Long>> sets = tpset.union(apset).union(gtRecorsNoSet);
        for (Tuple2<String, Long> i : sets.collect()) {
//            System.out.println("tammam "+i.f0);
            if (i.f0.equals("ap"))
                ap = i.f1;
            else if (i.f0.equals("tp"))
                tp = i.f1;
            else if (i.f0.equals("gt"))
                gtRecorsNo = i.f1;
        }
    }

    public long getTP() throws Exception {
        if (tp == -1)
            computeValues();
        return tp;
    }


    public long getAP() throws Exception {
        if(ap == -1)
            computeValues();
        return ap;
    }
    public long getGtRecordsNo() throws Exception {
        if(gtRecorsNo == -1)
            computeValues();
        return gtRecorsNo;
    }
    public Double computePrecision() throws Exception {
        if(tp == -1)
            computeValues();
        return (double)tp/ap;
    }
    public Double computeRecall() throws Exception {
        if(tp == -1)
            computeValues();
        return (double)tp/gtRecorsNo;    }

    public Double computeFM() throws Exception {
        if(tp == -1)
            computeValues();
        double pr = computePrecision();
        double re = computeRecall();
        return 2*pr*re/(pr+re);
    }
}
