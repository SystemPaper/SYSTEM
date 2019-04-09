package org.tool.system.linking.blocking.blocking_methods.data_structures;

import org.tool.system.linking.blocking.key_generation.BlockingKeyGenerator;

import java.io.Serializable;

/**
 */
public class StandardBlockingComponent extends BlockingComponent implements Serializable {
    private BlockingKeyGenerator blockingKeyGenerator;
    private Integer parallelismDegree;
    private Boolean inc_isNewNewCmpr = true;
    private Boolean inc_cmprCnflct = false;

//    public StandardBlockingComponent(
//    		Boolean IntraGraphComparison,
//    		BlockingMethod BlockingMethod,
//            BlockingKeyGenerator BlockingKeyGenerator,
//            Integer ParallelismDegree) {
//        super(IntraGraphComparison, BlockingMethod);
//        blockingKeyGenerator = BlockingKeyGenerator;
//        parallelismDegree = ParallelismDegree;
//    }


    public StandardBlockingComponent(
            Boolean IntraGraphComparison,
            Boolean inc_isNewNewCmpr,
            Boolean inc_cmprCnflct,
            BlockingMethod BlockingMethod,
            BlockingKeyGenerator BlockingKeyGenerator,
            Integer ParallelismDegree) {
        super(IntraGraphComparison, BlockingMethod);
        blockingKeyGenerator = BlockingKeyGenerator;
        parallelismDegree = ParallelismDegree;
        this.inc_isNewNewCmpr = inc_isNewNewCmpr;
        this.inc_cmprCnflct = inc_cmprCnflct;
    }
    public BlockingKeyGenerator getBlockingKeyGenerator(){return blockingKeyGenerator;}
    public Integer getParallelismDegree(){return parallelismDegree;}
    public Boolean getIsNewNewCmpr(){return inc_isNewNewCmpr;}
    public Boolean getIsCmprCnflct(){return inc_cmprCnflct;}


}
