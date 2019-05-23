package cn.escheduler.common.task.sdc;

import cn.escheduler.common.task.AbstractParameters;

import java.util.ArrayList;
import java.util.List;

public class SdcParameters extends AbstractParameters {
    private SdcStageConfiguration sourceStageConfig;
    private SdcStageConfiguration targetStageConfig;

    @Override
    public boolean checkParameters() {
        return true;
    }

    @Override
    public List<String> getResourceFilesList() {
        return new ArrayList<>();
    }

    public SdcStageConfiguration getSourceStageConfig() {
        return sourceStageConfig;
    }

    public void setSourceStageConfig(SdcStageConfiguration sourceStageConfig) {
        this.sourceStageConfig = sourceStageConfig;
    }

    public SdcStageConfiguration getTargetStageConfig() {
        return targetStageConfig;
    }

    public void setTargetStageConfig(SdcStageConfiguration targetStageConfig) {
        this.targetStageConfig = targetStageConfig;
    }
}
