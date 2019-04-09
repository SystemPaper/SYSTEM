package org.tool.system.example.EntityConfigMaker.util;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Vertex;


public class CorrectIncId implements MapFunction<Vertex, Vertex> {
    @Override
    public Vertex map(Vertex vertex) throws Exception {
        int incId = Integer.parseInt(vertex.getPropertyValue("inc").toString());
        if (incId == 0)
            vertex.setProperty("inc",1);
        else if (incId == 1)
            vertex.setProperty("inc", 2);
        else
            vertex.setProperty("inc", 0);
        return vertex;
    }
}