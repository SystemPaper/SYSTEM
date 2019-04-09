package org.tool.system.incremental.repairer.methods.ndr;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class MakeCurTuple2 implements MapFunction<Tuple1<String>, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> map(Tuple1<String> in) throws Exception {
        return Tuple2.of(in.f0,"*");
    }
}
