package cn.escheduler.common.task.sdc;

import java.util.List;

public class SdcStageConfiguration {
    private String libraryName;
    private String name;
    private int stageVersion;

    private List<SdcConfig> configValue;

    public SdcStageConfiguration(){

    }

    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStageVersion() {
        return stageVersion;
    }

    public void setStageVersion(int stageVersion) {
        this.stageVersion = stageVersion;
    }

    public List<SdcConfig> getConfigValue() {
        return configValue;
    }

    public void setConfigValue(List<SdcConfig> configValue) {
        this.configValue = configValue;
    }
}
