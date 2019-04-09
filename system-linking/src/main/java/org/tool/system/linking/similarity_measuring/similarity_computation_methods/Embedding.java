package org.tool.system.linking.similarity_measuring.similarity_computation_methods;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Computes the similarity of two strings using the Embedding method
 * Words and their corresponding vectors of a word-vector file are stored in a HashMap
 */
public class Embedding implements SimilarityComputation<String> {
    private static final String EMBEDDING_FILE_SEPARATOR = "\\s+";

    private static HashMap<File, HashMap<String, Float[]>> embeddings;
    private static HashMap<File, Integer> dimensions;
    private File wordVectorsFile;

    /**
     * @param wordVectorsFile the word-vector file
     * @throws IOException if the word-vector file can not be loaded
     */
    public Embedding(File wordVectorsFile) throws IOException {
        if (null == embeddings) {
            embeddings = new HashMap<>();
        }
        if (null == dimensions) {
            dimensions = new HashMap<>();
        }
        // Map each word-vector file to its dimension and word-vector values
        if (embeddings.get(wordVectorsFile) == null) {
            // Store dimension, expected as second field in first line
            String firstLine = Files.lines(wordVectorsFile.toPath()).findFirst().get();
            int dimension = Integer.parseInt(firstLine.split(EMBEDDING_FILE_SEPARATOR)[1]);
            dimensions.put(wordVectorsFile, dimension);

            // Store word vectors in HashMap
            HashMap<String, Float[]> wordVectorsOfFile = new HashMap<>();
            Files.lines(wordVectorsFile.toPath()).skip(1).forEach(line -> {
                String[] lineElements = line.split(EMBEDDING_FILE_SEPARATOR);
                String word = lineElements[0].toLowerCase();

                Float[] vector = Arrays.stream(lineElements)
                        .skip(1)
                        .filter(f -> NumberUtils.isNumber(f))
                        .map(m -> Float.parseFloat(m))
                        .toArray(size -> new Float[size]);

                wordVectorsOfFile.put(word, vector);
            });
            embeddings.put(wordVectorsFile, wordVectorsOfFile);
        }
        this.wordVectorsFile = wordVectorsFile;
    }

    public double computeSimilarity(String value1, String value2){
        try {
            return computeSimilarity(value1, value2, wordVectorsFile);
        } catch (Exception e) {

            e.printStackTrace();
            return 0d;
        }
    }

    /**
     * @param sentence1       the first string
     * @param sentence2       the second string
     * @param wordVectorsFile the word-vector file that is used to calculate similarity of the both strings
     * @return the similarity of the both strings
     * @throws Exception if the word-vector file can not be loaded
     */
    public double computeSimilarity(String sentence1, String sentence2, File wordVectorsFile) throws Exception {
        HashMap<String, Float[]> embeddingsForFile = embeddings.get(wordVectorsFile);
        int dimensionForFile = dimensions.get(wordVectorsFile);

        Float[] embeddingSentence1 = computeUnsupervisedSentenceVector(sentence1, embeddingsForFile, dimensionForFile);
        Float[] embeddingSentence2 = computeUnsupervisedSentenceVector(sentence2, embeddingsForFile, dimensionForFile);

        return (embeddingSentence1 == null || embeddingSentence2 == null) ? 0f : computeCosineSimilarity(embeddingSentence1, embeddingSentence2);
    }

    /**
     * Splits a sentence into words and calculates a vector representing
     * the whole sentence via normed average of single word vectors
     *
     * @param sentence   a string
     * @param embeddings the word vectors
     * @param dimension  dimension of vectors
     * @return the vector representing the whole sentence
     */
    private Float[] computeUnsupervisedSentenceVector(String sentence, HashMap<String, Float[]> embeddings, int dimension) {
        // Get vectors for all words in sentence
        List<Float[]> embeddingWords = new ArrayList<Float[]>();
        Pattern.compile(EMBEDDING_FILE_SEPARATOR).splitAsStream(sentence)
                .map(w -> w.toLowerCase().split(EMBEDDING_FILE_SEPARATOR))
                .forEach(e -> {
                    if (embeddings.containsKey(e[0].toLowerCase())) {
                        embeddingWords.add(embeddings.get(e[0].toLowerCase()));
                    }
                });

        // Calculate the vector of sentence using vectors from its words
        Float[] embeddingSentenceVector = new Float[dimension];
        Arrays.fill(embeddingSentenceVector, 0f);
        for (Float[] embeddingWord : embeddingWords) {
            if (embeddingWord == null || embeddingWord.length != dimension) return null;
            float norm = (float) computeVectorNorm(embeddingWord);
            for (int i = 0; i < embeddingWord.length; i++) {
                embeddingSentenceVector[i] += embeddingWord[i] / norm;
            }
        }
        embeddingSentenceVector = Arrays.stream(embeddingSentenceVector)
                .map(v -> v / embeddingWords.size())
                .toArray(size -> new Float[size]);

        return embeddingSentenceVector;
    }

    /**
     * @param vector a vector
     * @return the norm of this vector
     */
    private double computeVectorNorm(Float[] vector) {
        double sum = 0;
        for (int i = 0; i < vector.length; i++) {
            sum += vector[i] * vector[i];
        }
        return Math.sqrt(sum);
    }

    /**
     * @param vector1 the first vector
     * @param vector2 the second vector
     * @return the cosine similarity 2 vectors
     */
    private double computeCosineSimilarity(Float[] vector1, Float[] vector2) {
        double ab = 0;
        for (int i = 0; i < vector1.length; i++) {
            ab += vector1[i] * vector2[i];
        }

        double norm1 = computeVectorNorm(vector1);
        double norm2 = computeVectorNorm(vector2);

        return ab / (norm1 * norm2);
    }
}
