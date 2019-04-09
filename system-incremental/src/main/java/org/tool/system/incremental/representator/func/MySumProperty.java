package org.tool.system.incremental.representator.func;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.aggregation.functions.BaseAggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.Sum;

import java.util.Objects;

public class MySumProperty extends BaseAggregateFunction implements Sum {
    private final String propertyKey;

    public MySumProperty(String propertyKey) {
        this(propertyKey, /*"sum_" +*/ propertyKey);
    }

    public MySumProperty(String propertyKey, String aggregatePropertyKey) {
        super(aggregatePropertyKey);
        Objects.requireNonNull(propertyKey);
        this.propertyKey = propertyKey;
    }

    public PropertyValue getIncrement(Element element) {
            return element.getPropertyValue(this.propertyKey);

    }
}

