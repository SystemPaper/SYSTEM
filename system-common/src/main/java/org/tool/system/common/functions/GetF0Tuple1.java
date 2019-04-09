package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 */

public class GetF0Tuple1<T0> implements MapFunction <Tuple1<T0>, T0>{
    @Override
    public T0 map(Tuple1<T0> in) throws Exception {
        return in.f0;
    }
}
