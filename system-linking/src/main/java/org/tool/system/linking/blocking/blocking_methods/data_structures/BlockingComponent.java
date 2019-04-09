package org.tool.system.linking.blocking.blocking_methods.data_structures;

import java.io.Serializable;

/**
 */
public class BlockingComponent implements Serializable {
    private Boolean intraGraphComparison;
    private BlockingMethod blockingMethod;
    private Boolean inc_isNewNewCmpr = true;
    private Boolean inc_cmprCnflct = false;

    public BlockingComponent (Boolean intraGraphComparison, BlockingMethod blockingMethod){
        this.intraGraphComparison = intraGraphComparison;
        this.blockingMethod = blockingMethod;
    }
    public BlockingComponent (Boolean intraGraphComparison, BlockingMethod blockingMethod, Boolean inc_isNewNewCmpr, Boolean inc_cmprCnflct){
        this.intraGraphComparison = intraGraphComparison;
        this.blockingMethod = blockingMethod;
        this.inc_isNewNewCmpr = inc_isNewNewCmpr;
        this.inc_cmprCnflct = inc_cmprCnflct;
    }
    public BlockingMethod getBlockingMethod(){return blockingMethod;}
    public Boolean getIntraGraphComparison(){return intraGraphComparison;}
    public Boolean getIsNewNewCmpr(){return inc_isNewNewCmpr;}
    public Boolean getIsCmprCnflct(){return inc_cmprCnflct;}

}
