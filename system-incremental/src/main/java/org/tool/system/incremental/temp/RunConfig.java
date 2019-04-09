package org.tool.system.incremental.temp;

import org.tool.system.incremental.repairer.util.RepairMethod;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.IOException;


public class RunConfig {
    private Double aggThreshold;
    private String linkingConfFilePath;
    private Integer incrementLength;
    private String initialCollectionPath;
    private String tempOutPath;
    private RepairMethod repairMethod;
    private String resOutPath;
    private String vertexLabel;
    private String resOutInfo;
    /////////
    private String[] srcNames;
    private Integer srcNo;
    private Boolean isPreclustering;
    private Boolean isSrcConsistnt;
    private String iterative_TempFolder;
    private Integer initialDepth_IntDepth;
    private String GTFilePath;
    private Boolean isWithSameId;
    private String graphOutPath;
    private String allowedGraphIds;
    private Boolean isCSV;
    private String vertexGroupingKey;
    private String wccPropertyKeyList;
    private String repetitionAllowedList;
    private Boolean isEdgeAggregation;
    //////
    private String runId;
    private String id;





    public RunConfig(){
        isCSV = isEdgeAggregation = false;
        vertexGroupingKey = wccPropertyKeyList = repetitionAllowedList = "";
    }

    public RunConfig readConfig(String confFilePath, int runNo) throws ParserConfigurationException, IOException, SAXException {

        File fXmlFile = new File(confFilePath);
        Document doc  = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(fXmlFile);
        doc.getDocumentElement().normalize();
        Element root = (Element)doc.getElementsByTagName("runs").item(0);
        NodeList runConfigs = root.getElementsByTagName("runConfig");
        Element content = (Element)runConfigs.item(runNo);
        parseContent (content);
        return this;
    }

    private void parseContent(Element run) {
        if (run.getElementsByTagName("aggThreshold").getLength() !=0)
            aggThreshold = Double.parseDouble(run.getElementsByTagName("aggThreshold").item(0).getTextContent());
        linkingConfFilePath = run.getElementsByTagName("linkingConfFilePath").item(0).getTextContent();
        if (run.getElementsByTagName("incrementLength").getLength() !=0)
            incrementLength = Integer.parseInt(run.getElementsByTagName("incrementLength").item(0).getTextContent());
        if (run.getElementsByTagName("initialCollectionPath").getLength() !=0)
            initialCollectionPath = run.getElementsByTagName("initialCollectionPath").item(0).getTextContent();
        if (run.getElementsByTagName("tempOutPath").getLength() !=0)
            tempOutPath = run.getElementsByTagName("tempOutPath").item(0).getTextContent();
        if (run.getElementsByTagName("repairMethod").getLength() !=0)
            repairMethod = RepairMethod.valueOf(run.getElementsByTagName("repairMethod").item(0).getTextContent());
        if (run.getElementsByTagName("resOutPath").getLength() !=0)
            resOutPath = run.getElementsByTagName("resOutPath").item(0).getTextContent();
        if (run.getElementsByTagName("vertexLabel").getLength() !=0)
            vertexLabel = run.getElementsByTagName("vertexLabel").item(0).getTextContent();
        if (run.getElementsByTagName("resOutInfo").getLength() !=0)
            resOutInfo = run.getElementsByTagName("resOutInfo").item(0).getTextContent();
        /////////
        if (run.getElementsByTagName("srcNo").getLength() !=0)
            srcNo = Integer.parseInt(run.getElementsByTagName("srcNo").item(0).getTextContent());
        if (run.getElementsByTagName("srcNames").getLength() !=0)
            srcNames = run.getElementsByTagName("srcNames").item(0).getTextContent().split(",");
        if (run.getElementsByTagName("isPreclustering").getLength() !=0)
            isPreclustering = Boolean.parseBoolean(run.getElementsByTagName("isPreclustering").item(0).getTextContent());
        if (run.getElementsByTagName("isSrcConsistnt").getLength() !=0)
            isSrcConsistnt = Boolean.parseBoolean(run.getElementsByTagName("isSrcConsistnt").item(0).getTextContent());
        if (run.getElementsByTagName("ITTempFolder").getLength() !=0)
            iterative_TempFolder = run.getElementsByTagName("ITTempFolder").item(0).getTextContent();
        if (run.getElementsByTagName("IDIntDepth").getLength() !=0)
            initialDepth_IntDepth = Integer.parseInt(run.getElementsByTagName("IDIntDepth").item(0).getTextContent());
        if (run.getElementsByTagName("GTFilePath").getLength() !=0)
            GTFilePath = run.getElementsByTagName("GTFilePath").item(0).getTextContent();
        if (run.getElementsByTagName("isWithSameId").getLength() !=0)
            isWithSameId = Boolean.parseBoolean(run.getElementsByTagName("isWithSameId").item(0).getTextContent());
        if (run.getElementsByTagName("graphOutPath").getLength() !=0)
            graphOutPath = run.getElementsByTagName("graphOutPath").item(0).getTextContent();
        if (run.getElementsByTagName("allowedGraphIds").getLength() !=0)
            allowedGraphIds = run.getElementsByTagName("allowedGraphIds").item(0).getTextContent();
        if (run.getElementsByTagName("isCSV").getLength() !=0)
            isCSV = Boolean.parseBoolean(run.getElementsByTagName("isCSV").item(0).getTextContent());
        if (run.getElementsByTagName("vertexGroupingKey").getLength() !=0) {
            vertexGroupingKey = run.getElementsByTagName("vertexGroupingKey").item(0).getTextContent();
            wccPropertyKeyList = run.getElementsByTagName("wccPropertyKeyList").item(0).getTextContent();
            repetitionAllowedList = run.getElementsByTagName("repetitionAllowed").item(0).getTextContent();
        }
        if (run.getElementsByTagName("isEdgeAggregation").getLength() !=0)
            isEdgeAggregation = Boolean.parseBoolean(run.getElementsByTagName("isEdgeAggregation").item(0).getTextContent());


        //////
        runId = run.getElementsByTagName("runId").item(0).getTextContent();
        id = run.getElementsByTagName("id").item(0).getTextContent();

    }
    public Double getAggThreshold(){return aggThreshold;}
    public String getLinkingConfFilePath(){return linkingConfFilePath;}
    public Integer getIncrementLength(){return incrementLength;}
    public String getInitialCollectionPath(){return initialCollectionPath;}
    public Integer getSrcNo(){return srcNo;}
    public String getTempOutPath(){return tempOutPath;}
    public RepairMethod getRepairMethod(){return repairMethod;}
    public String getResOutPath(){return resOutPath;}
    public String getVertexLabel(){return vertexLabel;}
    public String getResOutInfo(){return resOutInfo;}
    /////////
    public String[] getSrcNames(){return srcNames;}
    public Boolean getIsPreclustering(){return isPreclustering;}
    public Boolean getIsSrcConsistnt(){return isSrcConsistnt;}
    public String getIterative_TempFolder(){return iterative_TempFolder;}
    public Integer getInitialDepth_IntDepth(){return initialDepth_IntDepth;}
    public String getGTFilePath(){return GTFilePath;}
    public Boolean getIsWithSameId(){return isWithSameId;}
    public String getGraphOutPath(){return graphOutPath;}
    public String getAllowedGraphIds(){return allowedGraphIds;}
    public Boolean getIsCSV(){return isCSV;}
    public String getVertexGroupingKey(){return vertexGroupingKey;}
    public String getWccPropertyKeyList(){return wccPropertyKeyList;}
    public Boolean[] getRepetitionAllowed(){
        String[] list = repetitionAllowedList.split(",");
        Boolean[] out = new Boolean[list.length];
        for (int i=0; i< list.length; i++)
            out[i] = Boolean.parseBoolean(list[i]);
        return out;
    }
    public Boolean getIsEdgeAggregation(){return isEdgeAggregation;}

    /////////
    public String getRunId(){return runId;}
    public String getId(){return id;}



}








































