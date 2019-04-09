package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;


@FunctionAnnotation.ForwardedFieldsFirst("f1->f0 , f2->f1")

public class GetF1F2Tuple4<T0, T1, T2, T3> implements MapFunction<Tuple4<T0, T1, T2, T3>, Tuple2<T1,T2>> {
    @Override
    public Tuple2<T1,T2> map(Tuple4<T0, T1, T2, T3> in) throws Exception {
        return Tuple2.of(in.f1, in.f2);
    }
}
