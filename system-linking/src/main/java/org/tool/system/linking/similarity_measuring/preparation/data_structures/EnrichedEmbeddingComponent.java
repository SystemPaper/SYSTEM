package org.tool.system.linking.similarity_measuring.preparation.data_structures;

import org.tool.system.linking.similarity_measuring.data_structures.EmbeddingComponent;
import org.tool.system.linking.similarity_measuring.data_structures.SimilarityComputationMethod;

import java.net.URI;

/**
 * Extended class from EmbeddingComponent, contains information to deal with FastText
 */
public class EnrichedEmbeddingComponent extends EmbeddingComponent {

    public static final String FASTTEXT_MODE_USE_WORD_VECTORS = "USE_WORD_VECTORS";
    public static final String FASTTEXT_MODE_USE_PRETRAINED_MODEL = "USE_PRETRAINED_MODEL";
    public static final String FASTTEXT_MODE_TRAIN_NEW_MODEL = "TRAIN_NEW_MODEL";
    public static final String FASTTEXT_TRAINING_MODE_CBOW = "CBOW";
    public static final String FASTTEXT_TRAINING_MODE_SKIPGRAM = "SKIPGRAM";

    private String fastTextModelMode;
    private String fastTextTrainingMode;
    private URI inputFileUri;
    private URI outputFileUri;
    private int trainingModelBucket;
    private int trainingModelDimension;
    private int trainingModelMinWordCount;

    public EnrichedEmbeddingComponent(String componentId,
                                      SimilarityComputationMethod similarityComputationMethod,
                                      String sourceGraphLabel,
                                      String srcLabel,
                                      String srcAttribute,
                                      String targetGraphLabel,
                                      String targetLabel,
                                      String targetAttribute,
                                      Double weight,
                                      String fastTextModelMode,
                                      String fastTextTrainingMode,
                                      String inputFileUri,
                                      String outputFileUri,
                                      Integer trainingModelBucket,
                                      Integer trainingModelDimension,
                                      Integer trainingModelMinWordCount) {
        super(componentId, similarityComputationMethod, sourceGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight);
        this.fastTextModelMode = fastTextModelMode;
        this.fastTextTrainingMode = fastTextTrainingMode;
        this.inputFileUri = URI.create(inputFileUri);
        this.outputFileUri = URI.create(outputFileUri);
        this.trainingModelBucket = trainingModelBucket;
        this.trainingModelDimension = trainingModelDimension;
        this.trainingModelMinWordCount = trainingModelMinWordCount;
    }

    public boolean isUseWordVectorsEnabled() {
        return fastTextModelMode.equals(FASTTEXT_MODE_USE_WORD_VECTORS);
    }

    public boolean isUsePretrainedModelEnabled() {
        return fastTextModelMode.equals(FASTTEXT_MODE_USE_PRETRAINED_MODEL);
    }

    public boolean isTrainModelEnabled() {
        return fastTextModelMode.equals(FASTTEXT_MODE_TRAIN_NEW_MODEL);
    }

    public boolean isUseCBOWTraining() {
        return fastTextTrainingMode.equals(FASTTEXT_TRAINING_MODE_CBOW);
    }

    public boolean isUseSkipgramTraining() {
        return fastTextTrainingMode.equals(FASTTEXT_TRAINING_MODE_SKIPGRAM);
    }

    public URI getInputFileUri() {
        return inputFileUri;
    }

    public URI getOutputFileUri() {
        return outputFileUri;
    }

    public int getTrainingModelDimension() {
        return trainingModelDimension;
    }

    public int getTrainingModelMinWordCount() {
        return trainingModelMinWordCount;
    }

    public int getTrainingModelBucket() {
        return trainingModelBucket;
    }
}
