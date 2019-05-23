package cn.escheduler.api.sdc;

import cn.escheduler.common.task.sdc.SdcStageConfiguration;
import cn.escheduler.common.utils.JSONUtils;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.PropertyFilter;
import com.google.common.collect.ImmutableSet;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.ConfigInjector;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeModule;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.stagelibrary.ClassLoaderStageLibraryTask;
import cn.escheduler.common.Constants;
import com.codahale.metrics.MetricRegistry;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.util.PipelineConfigurationUtil;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.Config;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageType;
import com.streamsets.pipeline.sdk.ElUtil;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static cn.escheduler.common.utils.PropertyUtils.getString;

public class PluginManager {
    private static final Logger logger = LoggerFactory.getLogger(PluginManager.class);

    private static volatile PluginManager INSTANCE = null;
    private ClassLoaderStageLibraryTask stageLibraryTask;
    private MetricRegistry registry = new MetricRegistry();
    private RuntimeInfo runtimeInfo;
    private BuildInfo buildInfo;
    private Map<String, Map<String, StageDefinition>> allStagesMap;
    private List<StageDisplayInfo> stageDisplayInfos;

    private PluginManager() {
    }

    public static PluginManager getInstance() {
        if (INSTANCE == null) {
            synchronized (PluginManager.class) {
                // when more than two threads run into the first null check same time, to avoid instanced more than one time, it needs to be checked again.
                if (INSTANCE == null) {
                    PluginManager tempInstance = new PluginManager();
                    //finish QuartzExecutors init
                    tempInstance.init();
                    INSTANCE = tempInstance;
                }
            }
        }
        return INSTANCE;
    }

    private void init() {
        String pluginPath = getString(Constants.SDC_PLUGIN_PATH);
        File pluginDir = new File(pluginPath);
        if (!pluginDir.isDirectory()) {
            logger.warn("SDC plugin directory not exists! {}", pluginPath);
        } else {
            List<DirClassLoader> classLoaders = Arrays.stream(pluginDir.listFiles(f -> f.isDirectory())).map(x -> {
                DirClassLoader dirClassLoader = new DirClassLoader(x, PluginManager.class.getClassLoader());
                return dirClassLoader;
            }).collect(Collectors.toList());

            RuntimeModule.setStageLibraryClassLoaders(classLoaders);
            RuntimeModule module = new RuntimeModule();
            runtimeInfo = module.provideRuntimeInfo(registry);
            buildInfo = module.provideBuildInfo();
            stageLibraryTask = new ClassLoaderStageLibraryTask(runtimeInfo, buildInfo, new Configuration());
            try {
                stageLibraryTask.init();
            } catch (Exception e) {
                logger.error("Stage Library Task init error, IGNORE for now.", e);
            }
            // init allStagesMap
            allStagesMap = new HashMap<>();
            stageLibraryTask.getStages().stream().forEach(l -> {
                logger.info("loaded library stage: {}", l);
                String libName = l.getLibrary();
                String stageName = l.getName();
                if (!allStagesMap.containsKey(libName)) {
                    allStagesMap.put(libName, new HashMap<>());
                }
                allStagesMap.get(libName).put(stageName, l);
            });
        }
        initStageDisplayInfos();
    }

    private void initStageDisplayInfos() {
        List<StageDisplayInfo> stageDisplayInfos = new ArrayList<>();
        allStagesMap.entrySet().forEach( libMapEntry -> {
            String libName = libMapEntry.getKey();
            libMapEntry.getValue().entrySet().forEach( stageMapEntry -> {
                String stageName = stageMapEntry.getKey();
                StageDefinition stage = stageMapEntry.getValue();
                StageDisplayInfo displayInfo = new StageDisplayInfo();
                displayInfo.setLibraryName(libName);
                displayInfo.setLibraryLabel(stage.getLibraryLabel());
                displayInfo.setName(stageName);
                displayInfo.setLabel(stage.getLabel());
                displayInfo.setType(stage.getType());
                try {
                    String iconBase64 = Base64.getEncoder().encodeToString(
                            IOUtils.toByteArray(stage.getStageClassLoader().getResource(stage.getIcon()))
                    );
                    displayInfo.setIconBase64(iconBase64);
                } catch (IOException e) {
                    logger.warn("read stage icon failed. ", e);
                }
                StageConfiguration defaultStageConfiguration = PipelineConfigurationUtil.getStageConfigurationWithDefaultValues(
                        stageLibraryTask,
                        libName,
                        stageName,
                        stageName + "-instance",
                        ""
                );
                displayInfo.setDefaultConfigurationJson(JSONUtils.toJson(defaultStageConfiguration.getConfiguration()));
                List<Map<String, String>> groupNames = stage.getConfigGroupDefinition().getGroupNameToLabelMapList();
                displayInfo.setGroupNames(groupNames);
                displayInfo.setStageVersion(stage.getVersion());
                displayInfo.setConfigurationDefinitionJson(
                        JSONObject.toJSONString(stage.getConfigDefinitions(), fastJsonConfigDefinitionFilter)
                );

                stageDisplayInfos.add(displayInfo);
            });
        });
        this.stageDisplayInfos = stageDisplayInfos;
    }

    private PropertyFilter fastJsonConfigDefinitionFilter = new PropertyFilter() {
        private Set<String> ignoreFieldNames = ImmutableSet.of(
                "dependsOn",
                "triggeredByValues",
                "elConstantDefinitions",
                "elConstantDefinitionsIdx",
                "elDefs",
                "elFunctionDefinitions",
                "elFunctionDefinitionsIdx",
                "annotatedType",
                "annotations",
                "configField",
                "configDefinitionsAsMap"
        );
        @Override
        public boolean apply(Object source, String name, Object value) {
            if (ignoreFieldNames.contains(name)) {
                return false;
            }
            return true;
        }
    };


    public List<StageDisplayInfo> getAllStages() {
        return stageDisplayInfos;
    }

    public StageDefinition getStageDefinition(SdcStageConfiguration stageConfig) {
        return allStagesMap.get(stageConfig.getLibraryName()).get(stageConfig.getName());
    }

    public Stage getStageInstance(SdcStageConfiguration stageConfig, Map<String, Object> pipelineConstants, List<Issue> errors) throws IllegalAccessException, InstantiationException {
        StageDefinition stage = getStageDefinition(stageConfig);

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(stage.getStageClassLoader());
            StageConfiguration config = PipelineConfigurationUtil.getStageConfigurationWithDefaultValues(
                    stageLibraryTask,
                    stage.getLibrary(),
                    stage.getName(),
                    stage.getName() + "-instance",
                    ""
            );

            stageConfig.getConfigValue().forEach(item ->
                    config.addConfig(new Config(item.getName(), item.getValue()))
            );

            Stage instance = stage.getStageClass().newInstance();
            ConfigInjector.get().injectStage(instance, stage, config, pipelineConstants, errors);
            return instance;
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    public StageContext createContext(
            StageDefinition stageDefinition,
            Class<?> stageClass,
            String instanceName,
            boolean isPreview,
            OnRecordError onRecordError,
            List<String> outputLanes,
            StageType stageType) {
        Map<String, Class<?>[]> configToElDefMap;
        if(stageClass == null) {
            configToElDefMap = Collections.emptyMap();
        } else {
            try {
                configToElDefMap = ElUtil.getConfigToElDefMap(stageClass);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return new StageContextWithServices(
                instanceName,
                stageType,
                0,
                isPreview,
                onRecordError,
                outputLanes,
                configToElDefMap,
                new HashMap<>(),
                ExecutionMode.STANDALONE,
                DeliveryGuarantee.AT_LEAST_ONCE,
                null,
                new EmailSender(new Configuration()),
                new Configuration(),
                new LineagePublisherDelegator.NoopDelegator(),
                runtimeInfo,
                stageDefinition
        );
    }

    public ServiceDefinition getServiceDefinition(Class c) {
        return stageLibraryTask.getServiceDefinition(c, true);
    }
}
