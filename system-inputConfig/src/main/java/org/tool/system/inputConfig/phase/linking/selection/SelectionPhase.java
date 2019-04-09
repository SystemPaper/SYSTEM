package org.tool.system.inputConfig.phase.linking.selection;

import org.tool.system.inputConfig.PhaseTitles;
import org.tool.system.linking.selection.data_structures.SelectionComponent;
import org.tool.system.inputConfig.phase.Phase;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.Collection;

/**
 */
public class SelectionPhase extends Phase{
	
    private Collection<Condition> conditions;
    private SelectionRule selectionRule;
    private SimilarityAggregatorRule similarityAggregatorRule;
    private Boolean aggregationRuleEnabled;
    private Boolean selectionRuleEnabled;
    
    public SelectionPhase(Element phaseContent){
        super(PhaseTitles.valueOf(phaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        NodeList phaseList = phaseContent.getElementsByTagName("Condition");
        for (int i = 0; i < phaseList.getLength(); i++) {
            Element subPhaseContent = (Element) phaseList.item(i);
            conditions.add(new Condition(subPhaseContent));
        }
        Element selectionRule = (Element)phaseContent.getElementsByTagName("SelectionRule");
        this.selectionRule = new SelectionRule(selectionRule);
        Element similarityAggregatorRule = (Element)phaseContent.getElementsByTagName("SimilarityAggregatorRule");
        this.similarityAggregatorRule = new SimilarityAggregatorRule(similarityAggregatorRule);
        
        aggregationRuleEnabled = Boolean.parseBoolean(phaseContent.getElementsByTagName("AggregationRuleEnabled").item(0).getTextContent());
        selectionRuleEnabled = Boolean.parseBoolean(phaseContent.getElementsByTagName("SelectionRuleEnabled").item(0).getTextContent());
    }
    public SelectionComponent toSelectionComponent(){
        Collection<org.tool.system.linking.selection.data_structures.Condition.Condition> linkingConditions = new ArrayList<>();
        for (Condition condition: conditions) {
        	linkingConditions.add(condition.toCondition());
        }
        
        return null;
        //new SelectionComponent(linkingConditions, selectionRule.toSelectionRule(), similarityAggregatorRule.toSimilarityAggregatorRule(), aggregationRuleEnabled, selectionRuleEnabled);
    }
}
