package org.tool.system.linking.similarity_measuring.preparation;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.util.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.tool.system.linking.similarity_measuring.preparation.data_structures.EnrichedEmbeddingComponent;
import org.tool.system.linking.similarity_measuring.preparation.functions.CollectPropertiesForCorpus;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import com.github.jfasttext.JFastText;

/**
 * Contains the methods related to FastText to train new model or use pre-trained model or use word vector
 */
public class EmbeddingTrainer {
    public static final String MODEL_TMP_FILE_PATTERN = "/tmp/%s-model-%s";
    public static final String CORPUS_TMP_FILE_PATTERN = "/tmp/%s-corpus-%s.txt";
    public static final String CORPUS_TMP_FILE_WORD_SEPARATOR = " ";
    public static final String HDFS_URI_SCHEME = "hdfs";

    private static JFastText jft;


    /**
     * @param c     a enriched embedding component
     * @param graph a logical graph
     * @throws Exception if the word-vector file or the temporary corpus can not be written
     *                   or word-vector file can not be put in HDFS
     */
    public static void runFastText(EnrichedEmbeddingComponent c, LogicalGraph graph) throws Exception {
        String wordVectorsFile = "";
        if (c.isUseWordVectorsEnabled()) {
            c.setWordVectorsFileUri(c.getInputFileUri());
            return;
        }

        if (c.isUsePretrainedModelEnabled()) {
            wordVectorsFile = usePreTrainedModel(c, graph);
        } else if (c.isTrainModelEnabled()) {
            String corpusTmpFilePath = writeCorpusToTmpFile(c, graph);
            wordVectorsFile = trainFastTextModel(c, corpusTmpFilePath);
        }
        c.setWordVectorsFileUri(URI.create(wordVectorsFile));

        if (c.getOutputFileUri().getScheme().equals(HDFS_URI_SCHEME)) {
            putToHDFS(wordVectorsFile, c.getOutputFileUri());
        }
    }

    /**
     * @param c     a enriched embedding component
     * @param graph a logical graph
     * @return path to the temporary corpus
     * @throws Exception if the temporary corpus can not be written
     */
    private static String writeCorpusToTmpFile(EnrichedEmbeddingComponent c, LogicalGraph graph) throws Exception {
        //words, that are written in temporary file, are words from the vertices of the prepared graph
        List<String> wordList = graph.getVertices().flatMap(new CollectPropertiesForCorpus(c)).collect();

        final String tmpCorpusFilePath = String.format(CORPUS_TMP_FILE_PATTERN, c.getId(),
                Long.toString(System.currentTimeMillis() / 1000L));
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(tmpCorpusFilePath))) {
            for (String s : wordList) {
                writer.write(s + CORPUS_TMP_FILE_WORD_SEPARATOR);
            }
        }

        return tmpCorpusFilePath;
    }

    /**
     * @param c              a enriched embedding component
     * @param corpusFilePath path to the temporary corpus
     * @return path to the word-vector file after training
     */
    private static String trainFastTextModel(EnrichedEmbeddingComponent c, String corpusFilePath) {
        String outputPath = c.getOutputFileUri().getRawPath();
        String trainingMode = c.isUseCBOWTraining() ? "cbow" : c.isUseSkipgramTraining() ? "skipgram" : "";
        if (c.getOutputFileUri().getScheme().equals(HDFS_URI_SCHEME)) {
            outputPath = String.format(MODEL_TMP_FILE_PATTERN, c.getId(),
                    Long.toString(System.currentTimeMillis() / 1000L));
        }

        getJftInstance().runCmd(new String[]{
                trainingMode,
                "-input", corpusFilePath,
                "-output", outputPath,
                "-bucket", Integer.toString(c.getTrainingModelBucket()),
                "-dim", Integer.toString(c.getTrainingModelDimension()),
                "-minCount", Integer.toString(c.getTrainingModelMinWordCount())
        });

        return outputPath + ".vec";
    }

    /**
     * @param c     a enriched embedding component
     * @param graph a logical graph
     * @return path to the word-vector file after parsing vector of each word in temporary corpus from model file
     * @throws Exception if the word-vector file can not be written
     */
    private static String usePreTrainedModel(EnrichedEmbeddingComponent c, LogicalGraph graph) throws Exception {
        String outputPath = c.getOutputFileUri().getRawPath();

        if (c.getOutputFileUri().getScheme().equals(HDFS_URI_SCHEME)) {
            outputPath = String.format(MODEL_TMP_FILE_PATTERN, c.getId(),
                    Long.toString(System.currentTimeMillis() / 1000L));
        }

        getJftInstance().loadModel(c.getInputFileUri().getRawPath());
        Set<String> wordList = new HashSet<String>();

        //prepare vertices for writing
        graph.getVertices().flatMap(new CollectPropertiesForCorpus(c))
                .distinct()
                .collect()
                .stream()
                .forEach(words -> wordList.addAll(Arrays.asList(words.split(CORPUS_TMP_FILE_WORD_SEPARATOR))));

        //get vector of word from FastText model file and write to word-vector file
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputPath))) {
            writer.write(wordList.size() + CORPUS_TMP_FILE_WORD_SEPARATOR + getJftInstance().getDim() + "\n");
            for (String s : wordList) {
                List<Float> vector = getJftInstance().getVector(s);
                writer.write(s + " ");
                for (Float f : vector) {
                    writer.write(f.toString() + " ");
                }
                writer.write("\n");
            }
        } finally {
            getJftInstance().unloadModel();
        }

        return outputPath;
    }

    /**
     * @param fileToPut   path of the file that is put in HDFS
     * @param destination HDFS URI where the file is saved
     * @throws IOException if the file can not be put in HDFS
     */
    private static void putToHDFS(String fileToPut, URI destination) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", destination.getRawPath());

        FileSystem fileSystem = FileSystem.get(destination, conf);
        Path destinationPath = new Path(new File(destination.getRawPath()).getParent());
        if (!fileSystem.exists(destinationPath)) {
            fileSystem.mkdirs(destinationPath);
        }
        FSDataOutputStream fsOutputStream = fileSystem.create(new Path(destination.getRawPath()));
        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(fileToPut)));
        IOUtils.copyBytes(inputStream, fsOutputStream);
        fsOutputStream.close();
        inputStream.close();
    }

    /**
     * @return the instance of JFastText
     */
    private static JFastText getJftInstance() {
        if (null == jft) {
            jft = new JFastText();
        }
        return jft;
    }
}
