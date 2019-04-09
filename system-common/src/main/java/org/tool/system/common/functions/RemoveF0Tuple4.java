package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;


@FunctionAnnotation.ForwardedFieldsFirst("f1->f0;f2->f1;f3->f2")

public class RemoveF0Tuple4<A, B, C, D> implements MapFunction<Tuple4<A, B, C, D>, Tuple3<B,C,D>> {
    @Override
    public Tuple3<B, C, D> map(Tuple4<A, B, C, D> value) throws Exception {
        return Tuple3.of(value.f1, value.f2, value.f3);
    }
}