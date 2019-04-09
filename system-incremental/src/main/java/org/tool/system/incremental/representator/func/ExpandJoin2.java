package org.tool.system.incremental.representator.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;



//edge_trgtId_srcSrc.join(vNewId_vOldId_src).where(1).equalTo(1).with(new ExpandJoin2());
public class ExpandJoin2 implements JoinFunction<Tuple3<Edge, String, String>, Tuple3<String, String, String>, Tuple2<Edge,Boolean>> {
    @Override
    public Tuple2<Edge,Boolean> join(Tuple3<Edge, String, String> in1, Tuple3<String, String, String> in2) throws Exception {
        in1.f0.setTargetId(new GradoopId().fromString(in2.f0));
        Boolean isOk = true;
        if(in1.f2.equals(in2.f2) || in1.f0.getSourceId().equals(in1.f0.getTargetId()))
            isOk = false;
        return Tuple2.of(in1.f0, isOk);
    }
}
