package org.tool.system.inputConfig;

import org.tool.system.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.tool.system.linking.blocking.blocking_methods.data_structures.BlockingMethod;
import org.tool.system.linking.blocking.blocking_methods.data_structures.StandardBlockingComponent;
import org.tool.system.linking.blocking.key_generation.BlockingKeyGenerator;
import org.tool.system.linking.blocking.key_generation.key_generation_methods.data_structures.KeyGenerationComponent;
import org.tool.system.linking.blocking.key_generation.key_generation_methods.data_structures.KeyGenerationMethod;
import org.tool.system.linking.blocking.key_generation.key_generation_methods.data_structures.PrefixLengthComponent;
import org.tool.system.linking.linking.data_structures.LinkerComponent;
import org.tool.system.linking.selection.SelectionMethod;
import org.tool.system.linking.selection.data_structures.Condition.Condition;
import org.tool.system.linking.selection.data_structures.Condition.ConditionOperator;
import org.tool.system.linking.selection.data_structures.SelectionComponent;
import org.tool.system.linking.selection.data_structures.SelectionRule.RuleComponent;
import org.tool.system.linking.selection.data_structures.SelectionRule.SelectionComponentType;
import org.tool.system.linking.selection.data_structures.SelectionRule.SelectionOperator;
import org.tool.system.linking.selection.data_structures.SelectionRule.SelectionRule;
import org.tool.system.linking.selection.data_structures.SimilarityAggregatorRule.AggregatorComponent;
import org.tool.system.linking.selection.data_structures.SimilarityAggregatorRule.SimilarityAggregatorRule;
import org.tool.system.inputConfig.phase.linking.similarity_measuring.SimilarityComponent;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class
LinkerConfig {
    private Double threshold;
    private List<Condition> conditions = new ArrayList<>();
    public LinkerConfig(Double aggThreshold){
        threshold = aggThreshold;
    }

    public List<LinkerComponent> readConfig(String confPath) throws ParserConfigurationException, IOException, SAXException {
        File fXmlFile = new File(confPath);
        List<LinkerComponent> linkerComponents = new ArrayList<>();
        Document doc  = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(fXmlFile);
        doc.getDocumentElement().normalize();
        Element root = (Element)doc.getElementsByTagName("linkings").item(0);
        NodeList linkingList = root.getElementsByTagName("linking");

        for (int i = 0; i < linkingList.getLength(); i++) {
            Element content = (Element) linkingList.item(i);
            linkerComponents.add(this.parseContent(content));
        }
        return linkerComponents;
    }

    private LinkerComponent parseContent(Element linking) {

        // BlockingComponents
//        Element linking = (Element) content.getElementsByTagName("linking").item(0);
        Element blocking = (Element) linking.getElementsByTagName("blocking").item(0);
//        BlockingMethod blockingMethod = BlockingMethod.valueOf(blocking.getElementsByTagName("method").item(0).getTextContent());
        boolean intraGraphComparison = Boolean.parseBoolean(blocking.getElementsByTagName("intraGraphComparison").item(0).getTextContent());
        String key = blocking.getElementsByTagName("key").item(0).getTextContent();
        Integer keyLength = Integer.parseInt(blocking.getElementsByTagName("keyLength").item(0).getTextContent());
        Integer parallelismDegree = Integer.parseInt(blocking.getElementsByTagName("parallelismDegree").item(0).getTextContent());
//        Integer parallelismDegree = 2;
        boolean isNewNewCmpr = true;
        boolean isFirstInc = false;

        if(blocking.getElementsByTagName("incremental").getLength()!=0){
            isNewNewCmpr =
                    Boolean.parseBoolean(((Element)blocking.getElementsByTagName("incremental").item(0)).getElementsByTagName("isNewNewCmpr").item(0).getTextContent());
            if (((Element)blocking.getElementsByTagName("incremental").item(0)).getElementsByTagName("isFirstInc").getLength()!=0)
                isFirstInc =
                    Boolean.parseBoolean(((Element)blocking.getElementsByTagName("incremental").item(0)).getElementsByTagName("isFirstInc").item(0).getTextContent());
        }

        KeyGenerationComponent keyGenerationComponent = new PrefixLengthComponent(KeyGenerationMethod.PREFIX_LENGTH, key, keyLength);
        BlockingKeyGenerator blockingKeyGenerator = new BlockingKeyGenerator(keyGenerationComponent);
        BlockingComponent blockingComponent = new StandardBlockingComponent
                (intraGraphComparison, isNewNewCmpr, isFirstInc, BlockingMethod.STANDARD_BLOCKING,blockingKeyGenerator,parallelismDegree);
        Collection<BlockingComponent> blockingComponents = new ArrayList<>();
        blockingComponents.add(blockingComponent);

        // SimilarityComponents
        NodeList similarities = linking.getElementsByTagName("similarityComponents");
        Collection<org.tool.system.linking.similarity_measuring.data_structures.SimilarityComponent> similarityComponents = new ArrayList<>();

        for (int i = 0; i < similarities.getLength(); i++) {
            Element similarity = (Element) similarities.item(i);
            org.tool.system.linking.similarity_measuring.data_structures.SimilarityComponent similarityComponent =
                    new SimilarityComponent(similarity).toSimilarityComponent();
            similarityComponents.add(similarityComponent);
        }

        // SelectionComponents
        Element selection = (Element) linking.getElementsByTagName("selection").item(0);
        SelectionComponent selectionComponent = getSelectionComponent(selection);

        // others
        Boolean keepCurrentEdges = Boolean.parseBoolean(linking.getElementsByTagName("keepCurrentEdges").item(0).getTextContent());
        Boolean recompSimiCurEdgs = Boolean.parseBoolean(linking.getElementsByTagName("recompSimiCurEdgs").item(0).getTextContent());
        String edgeLabel = linking.getElementsByTagName("edgeLabel").item(0).getTextContent();

        // U should define an incremental component
        String multiLinkStrategy = "ave";
        if(linking.getElementsByTagName("multiLinkStrategy").getLength()!=0) {
            multiLinkStrategy = linking.getElementsByTagName("multiLinkStrategy").item(0).getTextContent();
//                   ((Element) linking.getElementsByTagName("incremental").item(0)).getElementsByTagName("multiLinkStrategy").item(0).getTextContent();
        }
        Boolean repetitionAllowed = false;
        if(linking.getElementsByTagName("repetitionAllowed").getLength()!=0) {
            repetitionAllowed = Boolean.parseBoolean(linking.getElementsByTagName("repetitionAllowed").item(0).getTextContent());
//                   ((Element) linking.getElementsByTagName("incremental").item(0)).getElementsByTagName("multiLinkStrategy").item(0).getTextContent();
        }


        LinkerComponent linkerComponent =
                new LinkerComponent(blockingComponents, similarityComponents, selectionComponent, keepCurrentEdges, recompSimiCurEdgs, edgeLabel,
                        multiLinkStrategy, repetitionAllowed);
        return linkerComponent;
    }










    private SelectionComponent getSelectionComponent(Element configElement) {

        Boolean aggregationRuleEnabled = Boolean.parseBoolean(configElement.getElementsByTagName("aggregationRuleEnabled").item(0).getTextContent());
        Boolean selectionRuleEnabled = Boolean.parseBoolean(configElement.getElementsByTagName("selectionRuleEnabled").item(0).getTextContent());
        SelectionMethod selectionMethod = SelectionMethod.valueOf(configElement.getElementsByTagName("selectionMethod").item(0).getTextContent());


        SelectionComponent selectionComponent = new SelectionComponent(selectionMethod, aggregationRuleEnabled,
                getSimilarityAggregatorRule(configElement),
                selectionRuleEnabled,
                getSelectionConditions(configElement),
                getSelectionRule(configElement));


        return selectionComponent;
    }

    private static Collection<Condition> getSelectionConditions(Element configElement) {

        Collection<Condition> selectionConditions = new ArrayList<>();

        NodeList ruleComponents = configElement.getElementsByTagName("condition");


        for(int i = 0; i < ruleComponents.getLength(); i++) {

            Element inputRuleComponent = (Element) ruleComponents.item(i);

//            if(inputRuleComponent.getElementsByTagName("componentType").item(0).getTextContent().equals("CONDITION")) {
                String conditionId = inputRuleComponent.getElementsByTagName("conditionId").item(0).getTextContent();
                String relatedSimilarityFieldId = inputRuleComponent.getElementsByTagName("simFieldId").item(0).getTextContent();
                String conditionOperatorString = inputRuleComponent.getElementsByTagName("operator").item(0).getTextContent();
                Double conditionThreshold = Double.parseDouble(inputRuleComponent.getElementsByTagName("threshold").item(0).getTextContent());

                ConditionOperator conditionOperator = null;

                switch(conditionOperatorString) {
                    case "EQUAL":
                        conditionOperator = ConditionOperator.EQUAL;
                        break;
                    case "GREATER":
                        conditionOperator = ConditionOperator.GREATER;
                        break;
                    case "SMALLER":
                        conditionOperator = ConditionOperator.SMALLER;
                        break;
                    case "GREATER_EQUAL":
                        conditionOperator = ConditionOperator.GREATER_EQUAL;
                        break;
                    case "SMALLER_EQUAL":
                        conditionOperator = ConditionOperator.SMALLER_EQUAL;
                        break;
                    case "NOT_EQUAL":
                        conditionOperator = ConditionOperator.NOT_EQUAL;
                        break;
                    default:
                        break;
                }

                selectionConditions.add(new Condition(conditionId, relatedSimilarityFieldId, conditionOperator, conditionThreshold));
//            }
        }

        return selectionConditions;
    }

    private static SelectionRule getSelectionRule(Element configElement) {
        List<RuleComponent> ruleComponents = new ArrayList<>();

        NodeList inputRuleComponents = configElement.getElementsByTagName("ruleComponents");

        for(int i = 0; i < inputRuleComponents.getLength(); i++) {
            Element inputRuleComponent = (Element) inputRuleComponents.item(i);

            String componentType = inputRuleComponent.getElementsByTagName("componentType").item(0).getTextContent();

            SelectionComponentType ruleComponentType = null;
            String ruleComponentValue = null;

            switch(componentType) {
                case "OPEN_PARENTHESIS":
                    ruleComponentType = SelectionComponentType.OPEN_PARENTHESIS;
                    ruleComponentValue = "(";
                    break;
                case "CLOSE_PARENTHESIS":
                    ruleComponentType = SelectionComponentType.CLOSE_PARENTHESIS;
                    ruleComponentValue = ")";
                    break;
                case "SELECTION_OPERATOR_AND":
                    ruleComponentType = SelectionComponentType.SELECTION_OPERATOR;
                    ruleComponentValue = SelectionOperator.AND.toString();
                    break;
                case "SELECTION_OPERATOR_OR":
                    ruleComponentType = SelectionComponentType.SELECTION_OPERATOR;
                    ruleComponentValue = SelectionOperator.OR.toString();
                    break;
                case "CONDITION":
                    ruleComponentType = SelectionComponentType.CONDITION_ID;
                    ruleComponentValue = inputRuleComponent.getElementsByTagName("conditionId").item(0).getTextContent();
                    break;
                case "COMPUTED_EXPRESSION_TRUE":
                    ruleComponentType = SelectionComponentType.COMPUTED_EXPRESSION;
                    ruleComponentValue = "true";
                    break;
                default:
                    break;
            }

            ruleComponents.add(new RuleComponent(ruleComponentType, ruleComponentValue));
        }

        SelectionRule selectionRule = new SelectionRule(ruleComponents);

        return selectionRule;
    }

    private SimilarityAggregatorRule getSimilarityAggregatorRule(Element configElement) {

        // currently only aggregation threshold is used internally
        List<AggregatorComponent> aggregatorComponents = new ArrayList<>();
//        Double aggregationThreshold = Double.parseDouble(configElement.getElementsByTagName("aggThreshold").item(0).getTextContent());
        Double aggregationThreshold = this.threshold;

        SimilarityAggregatorRule similarityAggregatorRule = new SimilarityAggregatorRule(aggregatorComponents, aggregationThreshold);

        return similarityAggregatorRule;

    }
}
