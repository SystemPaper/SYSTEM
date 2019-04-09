package org.tool.system.linking.selection;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.linking.linking.data_structures.LinkerComponent;
import org.tool.system.linking.selection.data_structures.Condition.ConditionValue;
import org.tool.system.linking.selection.data_structures.Condition.ConditionValueList;
import org.tool.system.linking.selection.data_structures.SelectionComponent;
import org.tool.system.linking.similarity_measuring.data_structures.SimilarityField;
import org.tool.system.linking.similarity_measuring.data_structures.SimilarityFieldList;


import java.io.Serializable;
import java.math.BigDecimal;

/**
 */
public class Selector implements FlatMapFunction<Tuple3<Vertex, Vertex, SimilarityFieldList>, Tuple3<Vertex, Vertex, Double>>, Serializable {
	private LinkerComponent component;

	public Selector(LinkerComponent component) {
		this.component = component;
	}

	@Override
	public void flatMap(Tuple3<Vertex, Vertex, SimilarityFieldList> input, Collector<Tuple3<Vertex, Vertex, Double>> output) throws Exception {
		SelectionComponent selectionComponent = component.getSelectionComponent();

			double similaritiesSum = 0.0;
			double weightSum = 0.0;

			for (SimilarityField similarityField : input.f2.getSimilarityFields()) {
//
				if(similarityField.getSimilarityValue()!=-1) {
					similaritiesSum += (similarityField.getSimilarityValue() * similarityField.getWeight());
					weightSum += similarityField.getWeight();
				}

			}
			double aggSimilarites = similaritiesSum / weightSum;
			aggSimilarites = getExactDoubleResult(aggSimilarites);
			if (aggSimilarites >= selectionComponent.getSimilarityAggregatorRule().getAggregationThreshold()) {
			output.collect(Tuple3.of(input.f0, input.f1, aggSimilarites));
			}
	}

	private boolean check(ConditionValueList conditionValues) {
		for (ConditionValue conVal: conditionValues.getConditionValues())
			if (!conVal.getValue())
				return false;
		return true;
	}
	public double getExactDoubleResult(double value) {
		return new BigDecimal(value)
				.setScale(6, BigDecimal.ROUND_HALF_UP)
				.doubleValue();
	}
}
