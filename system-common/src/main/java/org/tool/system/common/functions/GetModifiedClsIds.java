package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class GetModifiedClsIds implements MapFunction <Tuple2<String, String>, Tuple1<String>> {
    @Override
    public Tuple1<String> map(Tuple2<String, String> in) throws Exception {
        return Tuple1.of(in.f0);
    }
}
