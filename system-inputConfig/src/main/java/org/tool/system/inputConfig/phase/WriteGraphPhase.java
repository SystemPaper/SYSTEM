package org.tool.system.inputConfig.phase;

import org.tool.system.inputConfig.PhaseTitles;
import org.w3c.dom.Element;

/**
 */
public class WriteGraphPhase extends Phase{
    private String outputPath;
    private Integer parallelismDegree;
    public WriteGraphPhase(Element PhaseContent) {
        super(PhaseTitles.valueOf(PhaseContent.getElementsByTagName("PhaseTitle").item(0).getTextContent()));
        outputPath = PhaseContent.getElementsByTagName("OutputPath").item(0).getTextContent();
        parallelismDegree = Integer.parseInt(PhaseContent.getElementsByTagName("ParallelismDegree").item(0).getTextContent());
    }
    public String getOutputPath(){return outputPath;}
    public Integer getParallelismDegree(){return parallelismDegree;}
}
