package org.tool.system.linking.similarity_measuring.data_structures;

import org.tool.system.linking.similarity_measuring.similarity_computation_methods.Embedding;
import org.tool.system.linking.similarity_measuring.similarity_computation_methods.SimilarityComputation;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;

/**
 * The embedding component, contains the word-vector file, that is used to calculate the similarity between 2 vectors
 * It also contains a key using for Flink's distributed cache
 */
public class EmbeddingComponent extends SimilarityComponent {

    private File wordVectorsFile;
    private URI wordVectorsFileUri;
    private String distributedCacheKey;

    public EmbeddingComponent(String componentId,
                              SimilarityComputationMethod similarityComputationMethod,
                              String sourceGraphLabel,
                              String srcLabel,
                              String srcAttribute,
                              String targetGraphLabel,
                              String targetLabel,
                              String targetAttribute,
                              Double weight) {
        super(componentId, similarityComputationMethod, sourceGraphLabel, srcLabel, srcAttribute, targetGraphLabel, targetLabel, targetAttribute, weight);
        this.distributedCacheKey = UUID.randomUUID().toString();
    }

    @Override
    public SimilarityComputation buildSimilarityComputation() {

        try {
            return new Embedding(wordVectorsFile);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String getDistributedCacheKey() {
        return distributedCacheKey;
    }

    public URI getWordVectorsFileUri() {
        return wordVectorsFileUri;
    }

    public void setWordVectorsFileUri(URI wordVectorsFileUri) {
        this.wordVectorsFileUri = wordVectorsFileUri;
    }

    public void setWordVectorsFile(File wordVectorsFile) {
        this.wordVectorsFile = wordVectorsFile;
    }

    public File getWordVectorsFile() {
        return wordVectorsFile;
    }
}
