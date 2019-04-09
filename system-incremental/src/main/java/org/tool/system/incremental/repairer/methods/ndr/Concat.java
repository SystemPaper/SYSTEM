package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class Concat implements MapFunction<Tuple2<String, String>, Tuple1<String>> {
    @Override
    public Tuple1<String> map(Tuple2<String, String> in) throws Exception {
        return Tuple1.of(in.f0+","+in.f1);
    }
}
