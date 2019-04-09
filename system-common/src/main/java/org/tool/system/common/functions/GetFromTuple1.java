package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

public class GetFromTuple1<T> implements MapFunction<Tuple1<T>, T> {
    @Override
    public T map(Tuple1<T>  t) throws Exception {
        return t.f0;
    }
}
