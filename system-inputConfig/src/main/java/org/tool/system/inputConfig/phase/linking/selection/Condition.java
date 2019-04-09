package org.tool.system.inputConfig.phase.linking.selection;

import org.tool.system.linking.selection.data_structures.Condition.ConditionOperator;
import org.w3c.dom.Element;

/**
 */
public class Condition {
    private String id;
    private String similarityFieldId;
    private ConditionOperator operator;
    private Double threshold;
    public Condition(Element PhaseContent){
        id = PhaseContent.getElementsByTagName("Id").item(0).getTextContent();
        similarityFieldId = PhaseContent.getElementsByTagName("SimilarityFieldId").item(0).getTextContent();
        operator = ConditionOperator.valueOf(PhaseContent.getElementsByTagName("ConditionOperator").item(0).getTextContent());
        threshold = Double.parseDouble(PhaseContent.getElementsByTagName("Threshold").item(0).getTextContent());
    }
    public org.tool.system.linking.selection.data_structures.Condition.Condition toCondition(){
        return new org.tool.system.linking.selection.data_structures.Condition.Condition(id, similarityFieldId, operator, threshold);
    }
}
