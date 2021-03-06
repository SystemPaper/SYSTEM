package org.tool.system.common.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 */
public class Convert2Tuple1<T> implements MapFunction<T, Tuple1<T>> {
    @Override
    public Tuple1<T> map(T t) throws Exception {
        return Tuple1.of(t);
    }
}
