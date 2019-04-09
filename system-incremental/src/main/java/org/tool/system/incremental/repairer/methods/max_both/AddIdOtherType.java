package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

public class AddIdOtherType implements FlatMapFunction <Tuple6<String, Double, Vertex, Vertex, String, String>,
        Tuple6<String, Double, Vertex, String, String, String>> {
    @Override
    public void flatMap(Tuple6<String, Double, Vertex, Vertex, String, String> in,
                        Collector<Tuple6<String, Double, Vertex, String, String, String>> collector) throws Exception {

        String src1 ="";
        String src2 ="";
        if(in.f2.hasProperty("type")){
            src1 = in.f2.getPropertyValue("type").toString();
            src2 = in.f3.getPropertyValue("type").toString();
        }
        else {
            src1 = in.f2.getPropertyValue("graphLabel").toString();
            src2 = in.f3.getPropertyValue("graphLabel").toString();
        }
        collector.collect(Tuple6.of(in.f0, in.f1, in.f2, in.f4, in.f2.getId().toString(), src2));
        collector.collect(Tuple6.of(in.f0, in.f1, in.f3, in.f5, in.f3.getId().toString(), src1));

    }
}
