package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple4;


@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")

public class GetF0Tuple4<T0, T1, T2, T3> implements MapFunction<Tuple4<T0, T1, T2, T3>, T0> {
    @Override
    public T0 map(Tuple4<T0, T1, T2, T3> in) throws Exception {
        return in.f0;
    }
}

