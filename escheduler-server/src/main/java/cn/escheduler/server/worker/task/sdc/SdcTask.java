package cn.escheduler.server.worker.task.sdc;

import com.streamsets.datacollector.record.RecordImpl;
import com.streamsets.datacollector.runner.BatchImpl;
import cn.escheduler.api.sdc.PluginManager;
import cn.escheduler.common.task.AbstractParameters;
import cn.escheduler.common.task.sdc.SdcParameters;
import cn.escheduler.server.worker.task.AbstractTask;
import cn.escheduler.server.worker.task.TaskProps;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SdcTask extends AbstractTask {
    /**
     *  sdc parameters
     */
    private SdcParameters sdcParameters;

    public SdcTask(TaskProps props, Logger logger) {
        super(props, logger);

        this.sdcParameters = JSONObject.parseObject(props.getTaskParams(), SdcParameters.class);

        if (!sdcParameters.checkParameters()) {
            throw new RuntimeException("sdc task params is not valid");
        }
    }

    @Override
    public void handle() throws Exception {
        // set the name of the current thread
        String threadLoggerInfoName = String.format("TaskLogInfo-%s", taskProps.getTaskAppId());
        Thread.currentThread().setName(threadLoggerInfoName);
        logger.info("sdc task params {}", taskProps.getTaskParams());

        if (sdcParameters.getSourceStageConfig() == null || sdcParameters.getTargetStageConfig() == null){
            logger.error("Source Stage / Target Stage is missing");
            exitStatusCode = -1;
            return;
        }

        PluginManager instance = PluginManager.getInstance();
        Map<String, Object> pipelineConstants = new HashMap<>();
        List<Issue> errors = new ArrayList<>();
        Stage sourceStage = null;
        Stage targetStage = null;
        try {
            sourceStage = instance.getStageInstance(sdcParameters.getSourceStageConfig(), pipelineConstants, errors);
            targetStage = instance.getStageInstance(sdcParameters.getTargetStageConfig(), pipelineConstants, errors);
            if (!errors.isEmpty()) {
                errors.forEach(error ->
                        logger.error("Get Pipeline error: {}", error)
                );
                exitStatusCode = -1;
                return;
            }

            // init source
            Source.Context srcContext = instance.createContext(instance.getStageDefinition(sdcParameters.getSourceStageConfig()), sourceStage.getClass(), sdcParameters.getSourceStageConfig().getName() + "-instance",
                    false, OnRecordError.STOP_PIPELINE, Lists.newArrayList("source_output"), StageType.SOURCE);
            Stage.Info srcInfo = ContextInfoCreator.createInfo(sdcParameters.getSourceStageConfig().getName(), sdcParameters.getSourceStageConfig().getStageVersion(), sdcParameters.getSourceStageConfig().getName() + "-instance");
            List<ConfigIssue> srcInitResult = sourceStage.init(srcInfo, srcContext);

            if (!srcInitResult.isEmpty()) {
                srcInitResult.forEach(error ->
                        logger.error("Init Source Stage error: {}", error)
                );
                exitStatusCode = -1;
                return;
            }

            Target.Context targetContext = instance.createContext(instance.getStageDefinition(sdcParameters.getTargetStageConfig()), targetStage.getClass(), sdcParameters.getTargetStageConfig().getName() + "-instance", false, OnRecordError.STOP_PIPELINE, Lists.newArrayList(), StageType.TARGET);
            Stage.Info targetInfo = ContextInfoCreator.createInfo(sdcParameters.getTargetStageConfig().getName(), sdcParameters.getTargetStageConfig().getStageVersion(), sdcParameters.getTargetStageConfig().getName() + "-instance");
            List<ConfigIssue> targetInitResult = targetStage.init(targetInfo, targetContext);
            if (!targetInitResult.isEmpty()) {
                targetInitResult.forEach(error ->
                        logger.error("Init Target Stage error: {}", error)
                );
                exitStatusCode = -1;
                return;
            }

            BatchMaker batchMaker = new BatchMakerImpl(Lists.newArrayList("source_output"));
            String result = ((Source) sourceStage).produce("", 1000, batchMaker);
            com.streamsets.datacollector.runner.EventSink sink = ((com.streamsets.datacollector.runner.StageContext) srcContext).getEventSink();
            logger.info("get records size in batch: " + ((BatchMakerImpl) batchMaker).getOutput().get("source_output").size());

            ((Target) targetStage).write(new BatchImpl(sdcParameters.getTargetStageConfig().getName() + "-instance", "", "", ((BatchMakerImpl) batchMaker).getOutput().get("source_output")));
            exitStatusCode = 0;
            return;
        } finally {
            if (sourceStage != null) {
                sourceStage.destroy();
            }
            if (targetStage != null) {
                targetStage.destroy();
            }
        }
    }

    @Override
    public AbstractParameters getParameters() {
        return sdcParameters;
    }

    static class BatchMakerImpl implements BatchMaker {

        private final List<String> outputLanes;
        private final Map<String, List<Record>> laneToRecordsMap;
        private final String singleLaneOutput;

        public BatchMakerImpl(List<String> outputLanes) {
            this.outputLanes = outputLanes;
            if(outputLanes.size() == 1) {
                singleLaneOutput = outputLanes.iterator().next();
            } else {
                singleLaneOutput = null;
            }
            laneToRecordsMap = new HashMap<>();
            //The output map should always have a key for all the defined output lanes, if the stage did not produce any record
            // for a lane, the value in the map should be an empty record list.
            for(String lane : outputLanes) {
                laneToRecordsMap.put(lane, new ArrayList<Record>());
            }
        }

        @Override
        public List<String> getLanes() {
            return outputLanes;
        }

        @Override
        public void addRecord(Record record, String... lanes) {
            if(lanes == null || lanes.length == 0) {
                List<Record> records = laneToRecordsMap.get(singleLaneOutput);
                if(records == null) {
                    records = new ArrayList<>();
                    laneToRecordsMap.put(singleLaneOutput, records);
                }
                records.add(((RecordImpl)record).clone());
                return;
            }
            for(String lane : lanes) {
                List<Record> records = laneToRecordsMap.get(lane);
                if(records == null) {
                    records = new ArrayList<>();
                    laneToRecordsMap.put(lane, records);
                }
                records.add(((RecordImpl)record).clone());
            }
        }

        public Map<String, List<Record>> getOutput() {
            return laneToRecordsMap;
        }
    }
}
