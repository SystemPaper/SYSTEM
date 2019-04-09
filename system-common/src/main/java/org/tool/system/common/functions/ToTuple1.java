package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")

public class ToTuple1<T0> implements MapFunction <T0, Tuple1<T0>>{
    @Override
    public Tuple1<T0> map(T0 in) throws Exception {
        return Tuple1.of(in);
    }
}
