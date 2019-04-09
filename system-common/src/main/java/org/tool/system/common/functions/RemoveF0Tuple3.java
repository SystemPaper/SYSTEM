package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0;f2->f1")

public class RemoveF0Tuple3<A, B, C> implements MapFunction<Tuple3<A, B, C>, Tuple2<B,C>> {
    @Override
    public Tuple2<B, C> map(Tuple3<A, B, C> value) throws Exception {
        return Tuple2.of(value.f1, value.f2);
    }
}