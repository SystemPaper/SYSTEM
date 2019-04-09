package org.tool.system.incremental.representator.func;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateDefaultValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.Sum;


public class MyCount extends BaseAggregateFunction implements Sum, AggregateDefaultValue {
    public MyCount() {
        super("count");
    }

    public MyCount(String aggregatePropertyKey) {
        super(aggregatePropertyKey);
    }

    public PropertyValue getIncrement(Element element) {
        if(element.hasProperty("count"))
            return PropertyValue.create(Long.parseLong(element.getPropertyValue("count").toString()));
        return PropertyValue.create(Long.valueOf(1L));
    }

    public PropertyValue getDefaultValue() {
        return PropertyValue.create(Long.valueOf(0L));
    }
}
