package org.tool.system.incremental.representator.func;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

import java.util.ArrayList;
import java.util.List;


public class MyAggregateListOfWccVertices implements VertexAggregateFunction {
    private final String wccPropertyKey;
    private final String listOfWccIDsPropertyKey;
    private boolean isRepetitionAllowed;

    public MyAggregateListOfWccVertices(String wccPropertyKey, Boolean isRepetitionAllowed) {
        this.wccPropertyKey = wccPropertyKey;
        this.listOfWccIDsPropertyKey = "vertices_" + this.wccPropertyKey;
        this.isRepetitionAllowed = isRepetitionAllowed;
    }

    public PropertyValue getIncrement(Element vertex) {
        ArrayList valueList = new ArrayList();
        if (vertex.hasProperty(this.wccPropertyKey))
            valueList.add(PropertyValue.create(vertex.getPropertyValue(this.wccPropertyKey).toString()));
        else if (vertex.hasProperty(this.listOfWccIDsPropertyKey))
            valueList.addAll(vertex.getPropertyValue(this.listOfWccIDsPropertyKey).getList());
        else
            valueList.add(PropertyValue.create(""));
        return PropertyValue.create(valueList);
    }

    public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
        List aggregateList = aggregate.getList();
        if (this.isRepetitionAllowed)
            aggregateList.addAll(increment.getList());
        else {
            List<PropertyValue> list = increment.getList();
            for (PropertyValue pv : list) {
                if (!aggregateList.contains(pv))
                    aggregateList.add(pv);
            }
        }

        aggregate.setList(aggregateList);
        return aggregate;
    }


    public String getAggregatePropertyKey() {
        return this.listOfWccIDsPropertyKey;
    }
}
