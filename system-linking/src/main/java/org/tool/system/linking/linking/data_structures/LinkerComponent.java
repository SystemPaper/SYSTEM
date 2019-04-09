package org.tool.system.linking.linking.data_structures;

import org.tool.system.linking.blocking.blocking_methods.data_structures.BlockingComponent;
import org.tool.system.linking.selection.data_structures.SelectionComponent;
import org.tool.system.linking.similarity_measuring.data_structures.SimilarityComponent;

import java.io.Serializable;
import java.util.Collection;

/**
 */
public class LinkerComponent implements Serializable{
    private Collection<BlockingComponent> blockingComponents;
    private Collection<SimilarityComponent> similarityComponents;
    private SelectionComponent selectionComponent;
    private Boolean keepCurrentEdges;
    private Boolean recomputeSimilarityForCurrentEdges;
    private String edgeLabel;
    private String multiLinkStrategy;
    private Boolean isRepetitionAllowed;


    public LinkerComponent(Collection<BlockingComponent> blockingComponents, Collection<SimilarityComponent> similarityComponents,
                           SelectionComponent selectionComponent, Boolean keepCurrentEdges, Boolean recomputeSimilarityForCurrentEdges,
                           String edgeLabel,
                           String multiLinkStrategy,
                           Boolean isRepetitionAllowed){
        this.blockingComponents = blockingComponents;
        this.similarityComponents = similarityComponents;
        this.selectionComponent = selectionComponent;
        this.keepCurrentEdges = keepCurrentEdges;
        this.recomputeSimilarityForCurrentEdges = recomputeSimilarityForCurrentEdges;
        this.edgeLabel = edgeLabel;
        this.multiLinkStrategy = multiLinkStrategy;
        this.isRepetitionAllowed = isRepetitionAllowed;
    }




    public Collection<BlockingComponent> getBlockingComponents(){return blockingComponents;}
    public Collection<SimilarityComponent> getSimilarityComponents(){return similarityComponents;}
    public SelectionComponent getSelectionComponent(){return selectionComponent;}
    public Boolean getKeepCurrentEdges(){return keepCurrentEdges;}
    public Boolean getRecomputeSimilarityForCurrentEdges(){return recomputeSimilarityForCurrentEdges;}
    public String getEdgeLabel(){return edgeLabel;}
    public String getMultiLinkStrategy(){return multiLinkStrategy;}
    public Boolean getIsRepetitionAllowed(){return isRepetitionAllowed;}

}
