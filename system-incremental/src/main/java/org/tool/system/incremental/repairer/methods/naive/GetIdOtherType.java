package org.tool.system.incremental.repairer.methods.naive;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.omg.CORBA.Object;

import java.util.List;

public class GetIdOtherType implements FlatMapFunction <Tuple3<Edge, Vertex, Vertex>, Tuple4<Edge, Vertex,String, String>> {
    @Override
    public void flatMap(Tuple3<Edge, Vertex, Vertex> in, Collector<Tuple4<Edge, Vertex, String, String>> collector) throws Exception {
        String src1 ="";
        String src2 ="";

//        if(in.f1.hasProperty("type")){
//            src1 = in.f1.getPropertyValue("type").toString();
//            src2 = in.f2.getPropertyValue("type").toString();
//        }
//        else
        if(in.f1.hasProperty("graphLabel"))
            src1 = in.f1.getPropertyValue("graphLabel").toString();
        else {
            List<PropertyValue> vertices_graphLabel = in.f1.getPropertyValue("vertices_graphLabel").getList();
            for (PropertyValue label:vertices_graphLabel){
                src1 += ","+label;
            }
            src1 = src1.substring(1);
        }


        if(in.f2.hasProperty("graphLabel"))
            src2 = in.f2.getPropertyValue("graphLabel").toString();
        else {
            List<PropertyValue> vertices_graphLabel = in.f2.getPropertyValue("vertices_graphLabel").getList();
            for (PropertyValue label:vertices_graphLabel){
                src2 += ","+label;
            }
            src2 = src2.substring(1);
        }

        collector.collect(Tuple4.of(in.f0, in.f1, in.f1.getId().toString(), src2));
        collector.collect(Tuple4.of(in.f0, in.f2, in.f2.getId().toString(), src1));

    }
}
//intE_S_T_id_src