package cn.escheduler.common.task.sdc;

import com.streamsets.pipeline.api.impl.Utils;

public class SdcConfig {
    private String name;
    private Object value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return Utils.format("Config[name='{}' value='{}']", getName(), getValue());
    }
}
