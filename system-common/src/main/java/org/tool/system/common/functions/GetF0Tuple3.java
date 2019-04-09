package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")

public class GetF0Tuple3<T0, T1, T2> implements MapFunction<Tuple3<T0, T1, T2>, T0> {
    @Override
    public T0 map(Tuple3<T0, T1, T2> in) throws Exception {
        return in.f0;
    }
}
