package org.tool.system.linking.selection.data_structures;

import org.tool.system.linking.selection.SelectionMethod;
import org.tool.system.linking.selection.data_structures.Condition.Condition;
import org.tool.system.linking.selection.data_structures.SelectionRule.SelectionRule;
import org.tool.system.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule;

import java.io.Serializable;
import java.util.Collection;

/**
 */
public class SelectionComponent implements Serializable {

	private SelectionMethod selectionMethod;
	private Boolean aggregationRuleEnabled;
	private SimilarityAggregatorRule similarityAggregatorRule;
	private Boolean selectionRuleEnabled;
	private Collection<Condition> conditions;
	private SelectionRule selectionRule;

	public SelectionComponent(
			SelectionMethod selectionMethod,
			Boolean aggregationRuleEnabled,
			SimilarityAggregatorRule similarityAggregatorRule,
			Boolean selectionRuleEnabled,
			Collection<Condition> conditions,
			SelectionRule selectionRule
	) {
		this.selectionMethod = selectionMethod;
		this.aggregationRuleEnabled = aggregationRuleEnabled;
		this.similarityAggregatorRule = similarityAggregatorRule;
		this.selectionRuleEnabled = selectionRuleEnabled;
		this.conditions = conditions;
		this.selectionRule = selectionRule;
	}

	public SelectionMethod getSelectionMethod() {
		return selectionMethod;
	}

	public Boolean getAggregationRuleEnabled() {
		return aggregationRuleEnabled;
	}

	public SimilarityAggregatorRule getSimilarityAggregatorRule() {
		return similarityAggregatorRule;
	}

	public Boolean getSelectionRuleEnabled() {
		return selectionRuleEnabled;
	}

	public Collection<Condition> getConditions() {
		return conditions;
	}

	public SelectionRule getSelectionRule() {
		return selectionRule;
	}

}

