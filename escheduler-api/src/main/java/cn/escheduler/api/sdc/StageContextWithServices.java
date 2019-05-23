package cn.escheduler.api.sdc;

import com.google.common.collect.Lists;
import com.streamsets.datacollector.config.ServiceConfiguration;
import com.streamsets.datacollector.config.ServiceDefinition;
import com.streamsets.datacollector.config.ServiceDependencyDefinition;
import com.streamsets.datacollector.config.StageDefinition;
import com.streamsets.datacollector.creation.ConfigInjector;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.lineage.LineagePublisherDelegator;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.runner.ServiceContext;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.datacollector.validation.Issue;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StageContextWithServices extends StageContext {
    private static Logger logger = LoggerFactory.getLogger(StageContextWithServices.class);

    private final Map<String, Class<?>[]> copiedConfigToElDefMap;
    private final EmailSender copiedEmailSender;
    private final int copiedRunnerId;
    private final Configuration copiedConfiguration;
    private Map<Class<?>, Object> serviceInstanceMap;

    public StageContextWithServices(
            final String instanceName,
            StageType stageType,
            int runnerId,
            boolean isPreview,
            OnRecordError onRecordError,
            List<String> outputLanes,
            Map<String, Class<?>[]> configToElDefMap,
            Map<String, Object> constants,
            ExecutionMode executionMode,
            DeliveryGuarantee deliveryGuarantee,
            String resourcesDir,
            EmailSender emailSender,
            Configuration configuration,
            LineagePublisherDelegator lineagePublisherDelegator,
            RuntimeInfo runtimeInfo,
            StageDefinition stageDefinition
    ) {
        super(
                instanceName,
                stageType,
                runnerId,
                isPreview,
                onRecordError,
                outputLanes,
                configToElDefMap,
                constants,
                executionMode,
                deliveryGuarantee,
                resourcesDir,
                emailSender,
                configuration,
                lineagePublisherDelegator,
                runtimeInfo,
                Collections.emptyMap()
        );

        this.copiedConfigToElDefMap = configToElDefMap;
        this.copiedEmailSender = emailSender;
        this.copiedRunnerId = runnerId;
        this.copiedConfiguration = configuration;

        serviceInstanceMap = new HashMap<>();
        stageDefinition.getServices().stream().forEach(service -> {
            try {
                serviceInstanceMap.put(service.getService(), getService(service));
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private Object getService(ServiceDependencyDefinition serviceDependencyDefinition) throws IllegalAccessException, InstantiationException {
        ServiceDefinition serviceDefinition = PluginManager.getInstance().getServiceDefinition(serviceDependencyDefinition.getService());
        Service service = serviceDefinition.getKlass().newInstance();
        ServiceContext serviceContext = new ServiceContext(
                this.copiedConfiguration,
                this.copiedConfigToElDefMap,
                this.getPipelineConstants(),
                this.copiedEmailSender,
                this.getMetrics(),
                this.pipelineId,
                this.rev,
                this.copiedRunnerId,
                this.stageInstanceName,
                serviceDefinition.getName(),
                this.getResourcesDirectory()
        );

        List<Config> configs = serviceDependencyDefinition.getConfiguration().entrySet()
                .stream()
                .map(entry -> new Config(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        ServiceConfiguration serviceConfiguration = new ServiceConfiguration(
                serviceDependencyDefinition.getService(),
                serviceDefinition.getVersion(),
                configs
        );

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(serviceDefinition.getStageClassLoader());

            List<Issue> issues = Lists.newArrayList();
            ConfigInjector.get().injectService(service, this.stageInstanceName, serviceDefinition, serviceConfiguration, this.getPipelineConstants(), issues);
            if (!issues.isEmpty()) {
                throw new RuntimeException("Service Config init error: " + issues.toString());
            }

            List<ConfigIssue> initResult = service.init(serviceContext);
            if (!initResult.isEmpty()) {
                throw new RuntimeException("Service init error: " + initResult.toString());
            }
            return service;
        } finally {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    @Override
    public <T> T getService(Class<? extends T> serviceInterface) {
        return (T)serviceInstanceMap.get(serviceInterface);
    }
}
