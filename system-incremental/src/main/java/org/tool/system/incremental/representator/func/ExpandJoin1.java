package org.tool.system.incremental.representator.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

//DataSet<Tuple3<Edge, String, String>> edge_trgtId_srcSrc =
//        vNewId_vOldId_src.join(edge_srcId_trgtId).where(1).equalTo(1).with(new ExpandJoin1());

public class ExpandJoin1 implements JoinFunction <Tuple3<String, String, String>, Tuple3<Edge, String, String>, Tuple3<Edge, String, String>> {

    @Override
    public Tuple3<Edge, String, String> join(Tuple3<String, String, String> in1, Tuple3<Edge, String, String> in2) throws Exception {
        in2.f0.setSourceId(new GradoopId().fromString(in1.f0));
        return Tuple3.of(in2.f0,in2.f2,in1.f2);
    }
}
