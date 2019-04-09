package org.tool.system.incremental.representator.func;


//edge_srcId_trgtId = edge_srcId_trgtId.map(new ModiFyLoopSimValues());

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;

public class ModiFyLoopSimValues implements MapFunction <Tuple3<Edge, String, String>, Tuple3<Edge, String, String>>{
    @Override
    public Tuple3<Edge, String, String> map(Tuple3<Edge, String, String> input) throws Exception {
        if (input.f0.hasProperty("count")) {
            input.f0.setProperty("value",
                    Double.parseDouble(input.f0.getPropertyValue("value").toString()) / Long.parseLong(input.f0.getPropertyValue("count").toString()));
            input.f0.removeProperty("count");
        }


        return Tuple3.of(input.f0, input.f1, input.f2);
    }
}
