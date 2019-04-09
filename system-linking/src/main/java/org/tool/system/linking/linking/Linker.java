package org.tool.system.linking.linking;

//import org.apache.commons.math.util.MultidimensionalCounter;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.linking.blocking.BlockMaker;
import org.tool.system.linking.blocking.blocking_methods.data_structures.StandardBlockingComponent;
import org.tool.system.linking.linking.functions.CreateLink;
import org.tool.system.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.tool.system.linking.linking.data_structures.LinkerComponent;
import org.tool.system.linking.linking.temp.ComputeSimGeo;
import org.tool.system.linking.linking.temp.ComputeSimM;
import org.tool.system.linking.linking.temp.ComputeSimP;
import org.tool.system.linking.similarity_measuring.data_structures.EmbeddingComponent;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;

import static org.simmetrics.builders.StringMetricBuilder.with;

/**
 */
public class Linker implements UnaryCollectionToCollectionOperator {
	private LinkerComponent linkerComponent;
	private String dataset;

	public Linker(LinkerComponent linkerComponent, String datset) {

		this.linkerComponent = linkerComponent;
		this.dataset = datset;
	}
	@Override
	public GraphCollection execute(GraphCollection input) {
		ExecutionEnvironment env = input.getConfig().getExecutionEnvironment();
		registerCachedFiles(env, linkerComponent);

	    DataSet<Vertex> vertices = input.getVertices();


		/************************BLOCKING***********************************/
		/*******************************************************************/
		DataSet<Tuple2<Vertex, Vertex>> blockedVertices = null;
		for (BlockingComponent blocking : linkerComponent.getBlockingComponents()) {
			DataSet<Tuple2<Vertex, Vertex>> pairedVertices = new BlockMaker(blocking).execute(vertices, linkerComponent.getSimilarityComponents(), dataset);
			if (blockedVertices == null)
				blockedVertices = pairedVertices;
			else
				blockedVertices = blockedVertices.union(pairedVertices);
		}


		/*************************POSTPROCESSING****************************/
		/*******************************************************************/
		Double threshold = linkerComponent.getSelectionComponent().getSimilarityAggregatorRule().getAggregationThreshold();
		DataSet<Tuple3<Vertex, Vertex, Double>> blockedVertices_similarityDegree = null;

		if (dataset.equals("P"))
			blockedVertices_similarityDegree = blockedVertices.flatMap(new ComputeSimP(threshold));
		else if (dataset.equals("G"))
			blockedVertices_similarityDegree = blockedVertices.flatMap(new ComputeSimGeo(threshold, false));

		else if (dataset.equals("M"))
			blockedVertices_similarityDegree = blockedVertices.flatMap(new ComputeSimM(threshold, false));




		DataSet<Edge> edges =
				blockedVertices_similarityDegree.map(new CreateLink(input.getConfig().getEdgeFactory(), linkerComponent.getEdgeLabel(
				)));


		if (linkerComponent.getKeepCurrentEdges() && !linkerComponent.getRecomputeSimilarityForCurrentEdges())
			edges = edges.union(input.getEdges());




		return input.getConfig().getGraphCollectionFactory().fromDataSets(input.getGraphHeads(), vertices, edges);
	}

	@Override
	public String getName() {
		return Linker.class.getName();
	}

	public static void registerCachedFiles(ExecutionEnvironment env, LinkerComponent linkerComponent) {
		linkerComponent.getSimilarityComponents().stream()
				.filter(c -> c instanceof EmbeddingComponent)
				.forEach(c -> {
					String wordVectorsFileUri = ((EmbeddingComponent) c).getWordVectorsFileUri().toString();
					env.registerCachedFile(wordVectorsFileUri, ((EmbeddingComponent) c).getDistributedCacheKey());
				});
	}
}








