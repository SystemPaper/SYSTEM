package org.tool.system.linking;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.java.operators.MapOperator;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.linking.blocking.blocking_methods.data_structures.*;
import org.tool.system.linking.blocking.key_generation.BlockingKeyGenerator;
import org.tool.system.linking.blocking.key_generation.key_generation_methods.data_structures.*;
import org.tool.system.linking.linking.Linker;
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
import org.tool.system.linking.similarity_measuring.data_structures.*;
import org.tool.system.linking.similarity_measuring.data_structures.QGramsComponent;
import org.tool.system.linking.similarity_measuring.preparation.EmbeddingTrainer;
import org.tool.system.linking.similarity_measuring.preparation.data_structures.EnrichedEmbeddingComponent;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.ListSimilarity;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class LinkingUtils {
    private static Boolean RECOMPUTE_SIMILARITY_FOR_CURRENT_EDGES = false;
    private static String GRAPH_LABEL_PROPERTY = "graphLabel";

    public static LogicalGraph runLinking(LogicalGraph graph, LinkerComponent linkerComponent) {
        GraphCollection inputCollection = graph.getConfig().getGraphCollectionFactory().fromGraph(graph);
        GraphCollection result_collection = inputCollection.callForCollection(new Linker(linkerComponent,""));
        return result_collection.getConfig().getLogicalGraphFactory()
                .fromDataSets(result_collection.getVertices(), result_collection.getEdges());
    }

    public static LogicalGraph runLinking(LogicalGraph graph, JsonObject config) throws Exception {
        Integer parallelismDegree = graph.getConfig().getExecutionEnvironment().getParallelism();
        LinkerComponent linkerComponent = getLinkerComponent(config, graph, parallelismDegree);
        return runLinking(graph, linkerComponent);
    }

    public static LogicalGraph runLinking(LogicalGraph graph, String path) throws Exception {
        return runLinking(graph, readJson(path));
    }

    public static LogicalGraph addGraphLabel(LogicalGraph in, String label) {
        MapOperator<Vertex, Vertex> graphVertexTmp = in.getVertices().map(v -> {
            v.setProperty(GRAPH_LABEL_PROPERTY, label);
            return v;
        });
        return in.getConfig().getLogicalGraphFactory().fromDataSets(graphVertexTmp, in.getEdges());
    }

    public static LinkerComponent getLinkerComponent(JsonObject linkerConfig, LogicalGraph graph, Integer parallelismDegree) {
        // blocking components
        Collection<BlockingComponent> blockingComponents = new ArrayList<>();
        JsonArray blockingConfigs = linkerConfig.getAsJsonArray("blockingComponents");

        for (int i = 0; i < blockingConfigs.size(); i++) {
            JsonObject blockingConfig = blockingConfigs.get(i).getAsJsonObject();
            blockingComponents.add(getBlockingComponent(blockingConfig, parallelismDegree));
        }

        // similarity components
        Collection<SimilarityComponent> similarityComponents = new ArrayList<>();
        JsonArray similarityConfigs = linkerConfig.getAsJsonArray("similarityComponents");

        for (int i = 0; i < similarityConfigs.size(); i++) {
            JsonObject similarityConfig = similarityConfigs.get(i).getAsJsonObject();
            similarityComponents.add(getSimilarityComponent(similarityConfig, linkerConfig));
        }

        //for each embedding component do its corresponding job (train new model, use pre-trained model or use word-vector
        similarityComponents.stream()
                .filter(c -> c instanceof EnrichedEmbeddingComponent)
                .forEach(c -> {
                    try {
                        EmbeddingTrainer.runFastText(((EnrichedEmbeddingComponent) c), graph);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        // selection component
        JsonObject selectionConfig = linkerConfig.get("selectionComponent").getAsJsonObject();
        SelectionComponent selectionComponent = getSelectionComponent(selectionConfig);

        // linker component
        String edgeLabel = linkerConfig.get("edgeLabel").getAsString();
        Boolean keepCurrentEdges = linkerConfig.get("keepCurrentEdges").getAsBoolean();

        // rearrange similarity components if a classifier (with a predefined feature order) is used
//        if (selectionComponent.getClassifier() != null) {
//            JsonArray mappings = selectionConfig.get("classifierFeatureMapping").getAsJsonArray();
//            Collection<SimilarityComponent> similarityComponentsRearranged = new ArrayList<>();
//            for (int i = 0; i < mappings.size(); i++) {
//                String mappedSimCompId = mappings.get(i).getAsString();
//                for (SimilarityComponent simComp : similarityComponents) {
//                    if (simComp.getId().equals(mappedSimCompId)) {
//                        similarityComponentsRearranged.add(simComp);
//                    }
//                }
//            }
//            if (similarityComponentsRearranged.size() != mappings.size()) {
//                throw new IllegalArgumentException("Missing similarity item for the classifier used!");
//            } else {
//                similarityComponents = similarityComponentsRearranged;
//            }
//        }

        return new LinkerComponent(
                blockingComponents,
                similarityComponents,
                selectionComponent,
                keepCurrentEdges,
                RECOMPUTE_SIMILARITY_FOR_CURRENT_EDGES,
                edgeLabel,
                "ave",
                false
        );
    }

    private static BlockingComponent getBlockingComponent(JsonObject blockingConfig, Integer parallelismDegree) {
        String blockingMethodString = blockingConfig.get("blockingMethod").getAsString();
        BlockingKeyGenerator blockingKeyGenerator = null;
        BlockingMethod blockingMethodName = null;
        Boolean intraDataSetComparison = true;
        BlockingComponent blockingComponent = null;

        switch (blockingMethodString) {
            case "CARTESIAN_PRODUCT":
                blockingMethodName = BlockingMethod.CARTESIAN_PRODUCT;
                blockingComponent = new CartesianProductComponent(true, blockingMethodName);
                break;
//            case "STANDARD_BLOCKING":
//                blockingMethodName = BlockingMethod.STANDARD_BLOCKING;
//                blockingKeyGenerator = new BlockingKeyGenerator(getKeyGenerationComponent(blockingConfig));
//                blockingComponent = new StandardBlockingComponent(
//                        intraDataSetComparison,
//                        blockingMethodName,
//                        blockingKeyGenerator,
//                        parallelismDegree);
//                break;
            case "SORTED_NEIGHBORHOOD":
                Integer windowSize = blockingConfig.get("windowSize").getAsInt();
                intraDataSetComparison = true;
                blockingMethodName = BlockingMethod.SORTED_NEIGHBORHOOD;
                blockingKeyGenerator = new BlockingKeyGenerator(getKeyGenerationComponent(blockingConfig));
                blockingComponent = new SortedNeighborhoodComponent(
                        intraDataSetComparison,
                        blockingMethodName,
                        blockingKeyGenerator,
                        windowSize);
                break;
            default:
                throw new AssertionError();
        }
        return blockingComponent;
    }

    private static KeyGenerationComponent getKeyGenerationComponent(JsonObject blockingConfig) {
        String keyGenerationMethodString = blockingConfig.getAsJsonObject("keyGenerationComponent").get("keyGenerationMethod").getAsString();
        String keyAttribute = blockingConfig.getAsJsonObject("keyGenerationComponent").get("attribute").getAsString();
        Integer prefixLength = blockingConfig.getAsJsonObject("keyGenerationComponent").get("prefixLength").getAsInt();
        Integer qgramNo = blockingConfig.getAsJsonObject("keyGenerationComponent").get("qgramNo").getAsInt();
        Double qgramThreshold = blockingConfig.getAsJsonObject("keyGenerationComponent").get("qgramThreshold").getAsDouble();
        String tokenizer = blockingConfig.getAsJsonObject("keyGenerationComponent").get("tokenizer").getAsString();

        KeyGenerationMethod keyGenerationMethod;
        KeyGenerationComponent keyGenerationComponent = null;

        switch (keyGenerationMethodString) {
            case "FULL_ATTRIBUTE":
                keyGenerationMethod = KeyGenerationMethod.FULL_ATTRIBUTE;
                keyGenerationComponent = new KeyGenerationComponent(keyGenerationMethod, keyAttribute);
                break;
            case "PREFIX_LENGTH":
                keyGenerationMethod = KeyGenerationMethod.PREFIX_LENGTH;
                keyGenerationComponent = new PrefixLengthComponent(keyGenerationMethod, keyAttribute, prefixLength);
                break;
            case "QGRAMS":
                keyGenerationMethod = KeyGenerationMethod.QGRAMS;
                keyGenerationComponent = new org.tool.system.linking.blocking.key_generation.key_generation_methods.data_structures.QGramsComponent(keyGenerationMethod, keyAttribute, qgramNo, qgramThreshold);
                break;
            case "WORD_TOKENIZER":
                keyGenerationMethod = KeyGenerationMethod.WORD_TOKENIZER;
                keyGenerationComponent = new WordTokenizerComponent(keyGenerationMethod, keyAttribute, tokenizer);
                break;
            default:
                break;
        }
        return keyGenerationComponent;
    }

    private static SimilarityComponent getSimilarityComponent(JsonObject similarityConfig, JsonObject configElement) {
        String similarityFieldId = similarityConfig.get("id").getAsString();
        String sourceGraph = similarityConfig.get("sourceGraph").getAsString();
        String targetGraph = similarityConfig.get("targetGraph").getAsString();
        String sourceLabel = similarityConfig.get("sourceLabel").getAsString();
        String targetLabel = similarityConfig.get("targetLabel").getAsString();
        String sourceAttribute = similarityConfig.get("sourceAttribute").getAsString();
        String targetAttribute = similarityConfig.get("targetAttribute").getAsString();
        Double weight = similarityConfig.get("weight").getAsDouble();
        String similarityMethodName = similarityConfig.get("similarityMethod").getAsString();
        Double threshold = similarityConfig.get("threshold").getAsDouble();
        Integer length = similarityConfig.get("length").getAsInt();
        Boolean padding = similarityConfig.get("padding").getAsBoolean();
        String tokenizer = similarityConfig.get("tokenizer").getAsString();
        Double jaroWinklerThreshold = similarityConfig.get("jaroWinklerThreshold").getAsDouble();
        Integer minLength = similarityConfig.get("minLength").getAsInt();
        Double maxToleratedDis = similarityConfig.get("maxToleratedDis").getAsDouble();
        Double maxToleratedPercentage = similarityConfig.get("maxToleratedPercentage").getAsDouble();
        String fastTextModelMode = similarityConfig.get("fastTextModelMode").getAsString();
        String fastTextTrainingMode = similarityConfig.get("fastTextTrainingMode").getAsString();
        String fastTextInputFileUri = similarityConfig.get("fastTextInputFileUri").getAsString();
        String fastTextOutputFileUri = similarityConfig.get("fastTextOutputFileUri").getAsString();
        Integer fastTextTrainingModelBucket = similarityConfig.get("fastTextTrainingModelBucket").getAsInt();
        Integer fastTextTrainingModelDimension = similarityConfig.get("fastTextTrainingModelDimension").getAsInt();
        Integer fastTextTrainingModelMinWordCount = similarityConfig.get("fastTextTrainingModelMinWordCount").getAsInt();

        JsonObject selComp = configElement.getAsJsonObject("selectionComponent");
        if (selComp.get("selectionMethod").getAsString().equals("MANUAL")) {
            // internally the same code is used for calculation of both arithmetic and weighted average
            // arithmetic average is calculated as weighted average with equal weights
            String aggregationStrategy = selComp.get("aggregationStrategy").getAsString();
            if (aggregationStrategy.equals("ARITHMETIC_AVERAGE")) {
                weight = 1.0;
            }
        }

        SimilarityComputationMethod similarityComputationMethod =
          SimilarityComputationMethod.valueOf(similarityMethodName);

        switch (similarityComputationMethod) {
            case JAROWINKLER:
                return new JaroWinklerComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, threshold);
            case TRUNCATE_BEGIN:
                return new TruncateComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, length);
            case TRUNCATE_END:
                return new TruncateComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, length);
            case EDIT_DISTANCE:
                return new EditDistanceComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight);
            case QGRAMS:
                return new QGramsComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, length, padding, getSecondMethod(similarityConfig));
            case MONGE_ELKAN:
                return new MongeElkanComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, tokenizer, threshold);
            case EXTENDED_JACCARD:
               return new ExtendedJaccardComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, tokenizer, threshold, jaroWinklerThreshold);
            case LONGEST_COMMON_SUBSTRING:
                return new LongestCommSubComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, minLength, getSecondMethod(similarityConfig));
            case NUMERICAL_SIMILARITY_MAXDISTANCE:
                return new NumericalSimilarityWithMaxDistanceComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, maxToleratedDis);
            case NUMERICAL_SIMILARITY_MAXPERCENTAGE:
                return new NumericalSimilarityWithMaxPercentageComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, maxToleratedPercentage);
            case EMBEDDING:
                return new EnrichedEmbeddingComponent(
                        similarityFieldId, similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute, targetGraph, targetLabel, targetAttribute, weight, fastTextModelMode, fastTextTrainingMode, fastTextInputFileUri, fastTextOutputFileUri, fastTextTrainingModelBucket, fastTextTrainingModelDimension, fastTextTrainingModelMinWordCount);
        case LIST_SIMILARITY:
                ListSimilarity.AggregationMethod aM = ListSimilarity
                  .AggregationMethod.fromString(similarityConfig.get("aggregationStrategy").getAsString());

                SimilarityComponent elementComponent =
                  getSimilarityComponent(similarityConfig.getAsJsonObject("elementSimilarityComponent"),
                    new JsonObject());

                return new ListSimilarityComponent(similarityFieldId,
                  similarityComputationMethod, sourceGraph, sourceLabel, sourceAttribute,
                  targetGraph, targetLabel, targetAttribute, weight,
                  elementComponent, aM);


        default:
                throw new IllegalArgumentException("Unknown SimilarityComputation Method");
        }
    }

    private static SimilarityComputationMethod getSecondMethod(JsonObject similarityConfig) {
        String secondMethodName = similarityConfig.get("secondMethod").getAsString();

        SimilarityComputationMethod secondMethod = null;

        switch (secondMethodName) {
            case "OVERLAP":
                secondMethod = SimilarityComputationMethod.OVERLAP;
                break;
            case "JACARD":
                secondMethod = SimilarityComputationMethod.JACARD;
                break;
            case "DICE":
                secondMethod = SimilarityComputationMethod.DICE;
                break;
            default:
                break;
        }
        return secondMethod;
    }

    private static SelectionComponent getSelectionComponent(JsonObject selectionConfig) {
        SelectionMethod selectionMethod = selectionConfig.get("selectionMethod").getAsString().equals("AUTO") ?
                SelectionMethod.CLASSIFIER : SelectionMethod.RULE;

        Boolean aggregationRuleEnabled = selectionConfig.get("aggregationRuleEnabled").getAsBoolean();
        Boolean selectionRuleEnabled = selectionConfig.get("selectionRuleEnabled").getAsBoolean();

//        ClassifierCommon classifier = (selectionMethod == SelectionMethod.CLASSIFIER) &&
//                selectionConfig.get("classifierSource").getAsString().equals("PRECOMPUTED") ?
//                getClassifier(selectionConfig) : null;

        SelectionComponent selectionComponent;
        try {
            selectionComponent = new SelectionComponent(
                    selectionMethod,
                    aggregationRuleEnabled,
                    aggregationRuleEnabled ? getSimilarityAggregatorRule(selectionConfig) : null,
                    selectionRuleEnabled,
                    selectionRuleEnabled ? getSelectionConditions(selectionConfig) : null,
                    selectionRuleEnabled ? getSelectionRule(selectionConfig) : null
            );
        } catch (Exception e) {
            return null;
        }
        return selectionComponent;
    }

//    private static ClassifierCommon getClassifier(JsonObject selectionConfig) {
//        String classifierUri = selectionConfig.get("classifierUri").getAsString();
//        try {
//            return TrainingUtils.loadClassifierFromFile(classifierUri);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }
//    }

    private static Collection<Condition> getSelectionConditions(JsonObject selectionConfig) {

        Collection<Condition> selectionConditions = new ArrayList<>();

        JsonArray inputRuleComponents = selectionConfig.getAsJsonArray("ruleComponents");

        for (int i = 0; i < inputRuleComponents.size(); i++) {
            JsonObject inputRuleComponent = inputRuleComponents.get(i).getAsJsonObject();

            if (inputRuleComponent.get("componentType").getAsString().equals("CONDITION")) {
                String conditionId = inputRuleComponent.get("conditionId").getAsString();
                String relatedSimilarityFieldId = inputRuleComponent.get("similarityFieldId").getAsString();
                String conditionOperatorString = inputRuleComponent.get("operator").getAsString();
                Double conditionThreshold = inputRuleComponent.get("threshold").getAsDouble();

                ConditionOperator conditionOperator = null;

                switch (conditionOperatorString) {
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
            }
        }
        return selectionConditions;
    }

    private static SelectionRule getSelectionRule(JsonObject selectionConfig) {
        List<RuleComponent> ruleComponents = new ArrayList<>();

        JsonArray inputRuleComponents = selectionConfig.getAsJsonArray("ruleComponents");

        for (int i = 0; i < inputRuleComponents.size(); i++) {
            JsonObject inputRuleComponent = inputRuleComponents.get(i).getAsJsonObject();

            String componentType = inputRuleComponent.get("componentType").getAsString();

            SelectionComponentType ruleComponentType = null;
            String ruleComponentValue = null;

            switch (componentType) {
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
                    ruleComponentValue = inputRuleComponent.get("conditionId").getAsString();
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
        return new SelectionRule(ruleComponents);
    }

    private static SimilarityAggregatorRule getSimilarityAggregatorRule(JsonObject selectionConfig) {
        // currently only aggregation threshold is used internally
        List<AggregatorComponent> aggregatorComponents = new ArrayList<>();
        Double aggregationThreshold = selectionConfig.get("aggregationThreshold").getAsDouble();
        return new SimilarityAggregatorRule(aggregatorComponents, aggregationThreshold);
    }

    private static JsonObject readJson(String path) throws IOException {
        FileReader reader = new FileReader(path);
        return new JsonParser().parse(reader).getAsJsonObject();
    }
}
