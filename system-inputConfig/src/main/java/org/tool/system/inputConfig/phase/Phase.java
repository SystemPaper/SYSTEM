package org.tool.system.inputConfig.phase;

import org.tool.system.inputConfig.PhaseTitles;

/**
 */
public class Phase {
    protected PhaseTitles phaseTitle;

    public Phase(PhaseTitles PhaseTitle){
        phaseTitle = PhaseTitle;
    }
    public PhaseTitles getPhaseTitle(){
        return phaseTitle;
    }
}
