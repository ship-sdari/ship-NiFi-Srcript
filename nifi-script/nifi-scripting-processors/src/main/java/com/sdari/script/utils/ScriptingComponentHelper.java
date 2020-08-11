/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sdari.script.utils;

import com.sdari.script.processors.MyInvokeScriptedProcessor;
import org.apache.nifi.components.*;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.nifi.processor.ProcessContext;
import com.sdari.script.processors.ScriptEngineConfigurator;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains variables and methods common to scripting processors, reporting tasks, etc.
 */
public class ScriptingComponentHelper {

    public PropertyDescriptor SCRIPT_ENGINE;

    // A map from engine name to a custom configurator for that engine
    public final Map<String, ScriptEngineConfigurator> scriptEngineConfiguratorMap = new ConcurrentHashMap<>();
    public final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private static final Logger logger = LoggerFactory.getLogger(MyInvokeScriptedProcessor.class);
    public Map<String, ScriptEngineFactory> scriptEngineFactoryMap;
    private String scriptEngineName;
    private String scriptPath;
    private String scriptBody;
    private DBCPService dbcpService;
    private String processorId;
    private String[] modules;
    private List<PropertyDescriptor> descriptors;
    private List<AllowableValue> engineAllowableValues;

    public BlockingQueue<ScriptEngine> engineQ = null;

    public String getScriptEngineName() {
        return scriptEngineName;
    }

    public void setScriptEngineName(String scriptEngineName) {
        this.scriptEngineName = scriptEngineName;
    }

    public String getScriptPath() {
        return this.scriptPath;
    }

    public DBCPService getDBCPService() {
        return this.dbcpService;
    }

    public String getProcessorId() {
        return this.processorId;
    }

    public void setDBCPService(DBCPService dbcpService) {
        this.dbcpService = dbcpService;
    }

    public void setScriptPath(String scriptPath) {
        this.scriptPath = scriptPath;
    }

    public String getScriptBody() {
        return scriptBody;
    }

    public void setScriptBody(String scriptBody) {
        this.scriptBody = scriptBody;
    }

    public String[] getModules() {
        return modules;
    }

    public void setModules(String[] modules) {
        this.modules = modules;
    }

    public List<PropertyDescriptor> getDescriptors() {
        return getDescriptorsByShip(this.descriptors);
    }

    public List<AllowableValue> getScriptEngineAllowableValues() {
        return engineAllowableValues;
    }

    /**
     * 获取自定义的属性
     *
     * @return List<PropertyDescriptor>
     */
    public List<PropertyDescriptor> getDescriptorsByShip(List<PropertyDescriptor> descriptors) {
        Map<String, Boolean> propertyMap = new HashMap<>();
        try {
            for (PropertyDescriptor descriptor : descriptors) {
                propertyMap.put(descriptor.getName(), true);
            }
            if (descriptors.size() > 0 && propertyMap.size() > 0) {
                if (!propertyMap.containsKey(ScriptingComponentUtils.DBCP_SERVICE.getName())) {
                    descriptors.add(ScriptingComponentUtils.DBCP_SERVICE);
                }
                if (!propertyMap.containsKey(ScriptingComponentUtils.QUERY_SCRIPTName_SQL.getName())) {
                    descriptors.add(ScriptingComponentUtils.QUERY_SCRIPTName_SQL);
                }
                if (!propertyMap.containsKey(ScriptingComponentUtils.SCRIPT_PROCESSOR_ID.getName())) {
                    descriptors.add(ScriptingComponentUtils.SCRIPT_PROCESSOR_ID);
                }

            }
        } catch (Exception e) {
            logger.error("getDescriptorsByShip", e);
        }
        return descriptors;
    }

    /**
     * Custom validation for ensuring exactly one of Script File or Script Body is populated
     *
     * @param validationContext provides a mechanism for obtaining externally
     *                          managed values, such as property values and supplies convenience methods
     *                          for operating on those values
     * @return A collection of validation results
     */
    public Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();

        // Verify that exactly one of "script file" or "script body" is set
        Map<PropertyDescriptor, String> propertyMap = validationContext.getProperties();
        if (StringUtils.isEmpty(propertyMap.get(ScriptingComponentUtils.SCRIPT_FILE)) == StringUtils.isEmpty(propertyMap.get(ScriptingComponentUtils.SCRIPT_BODY))) {
            results.add(new ValidationResult.Builder().subject("Script Body or Script File").valid(false).explanation(
                    "exactly one of Script File or Script Body must be set").build());
        }

        return results;
    }

    /**
     * This method creates all resources needed for the script processor to function, such as script engines,
     * script file reloader threads, etc.
     */
    public void createResources() {
        descriptors = new ArrayList<>();
        // The following is required for JRuby, should be transparent to everything else.
        // Note this is not done in a ScriptEngineConfigurator, as it is too early in the lifecycle. The
        // setting must be there before the factories/engines are loaded.
        System.setProperty("org.jruby.embed.localvariable.behavior", "persistent");

        // Create list of available engines
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> scriptEngineFactories = scriptEngineManager.getEngineFactories();
        if (scriptEngineFactories != null) {
            scriptEngineFactoryMap = new HashMap<>(scriptEngineFactories.size());
            List<AllowableValue> engineList = new LinkedList<>();
            for (ScriptEngineFactory factory : scriptEngineFactories) {
                engineList.add(new AllowableValue(factory.getLanguageName()));
                scriptEngineFactoryMap.put(factory.getLanguageName(), factory);
            }

            // Sort the list by name so the list always looks the same.
            engineList.sort((o1, o2) -> {
                if (o1 == null) {
                    return o2 == null ? 0 : 1;
                }
                if (o2 == null) {
                    return -1;
                }
                return o1.getValue().compareTo(o2.getValue());
            });

            engineAllowableValues = engineList;
            AllowableValue[] engines = engineList.toArray(new AllowableValue[0]);

            SCRIPT_ENGINE = new PropertyDescriptor.Builder()
                    .name("Script Engine")
                    .required(true)
                    .description("The engine to execute scripts")
                    .allowableValues(engines)
                    .defaultValue(engines[0].getValue())
                    .required(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .build();
            descriptors.add(SCRIPT_ENGINE);
        }

        descriptors.add(ScriptingComponentUtils.SCRIPT_FILE);
        descriptors.add(ScriptingComponentUtils.SCRIPT_BODY);
        descriptors.add(ScriptingComponentUtils.MODULES);
        descriptors = getDescriptorsByShip(descriptors);

        isInitialized.set(true);
    }

    /**
     * Determines whether the given path refers to a valid file
     *
     * @param path a path to a file
     * @return true if the path refers to a valid file, false otherwise
     */
    public static boolean isFile(final String path) {
        return path != null && Files.isRegularFile(Paths.get(path));
    }

    /**
     * Performs common setup operations when the processor is scheduled to run. This method assumes the member
     * variables associated with properties have been filled.
     *
     * @param numberOfScriptEngines number of engines to setup
     */
    public void setup(int numberOfScriptEngines, ComponentLog log) {

        if (scriptEngineConfiguratorMap.isEmpty()) {
            ServiceLoader<ScriptEngineConfigurator> configuratorServiceLoader =
                    ServiceLoader.load(ScriptEngineConfigurator.class);
            for (ScriptEngineConfigurator configurator : configuratorServiceLoader) {
                scriptEngineConfiguratorMap.put(configurator.getScriptEngineName().toLowerCase(), configurator);
            }
        }
        setupEngines(numberOfScriptEngines, log);
    }

    /**
     * Configures the specified script engine. First, the engine is loaded and instantiated using the JSR-223
     * javax.script APIs. Then, if any script configurators have been defined for this engine, their init() method is
     * called, and the configurator is saved for future calls.
     *
     * @param numberOfScriptEngines number of engines to setup
     * @see ScriptEngineConfigurator
     */
    protected void setupEngines(int numberOfScriptEngines, ComponentLog log) {
        engineQ = new LinkedBlockingQueue<>(numberOfScriptEngines);
        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (StringUtils.isBlank(scriptEngineName)) {
                throw new IllegalArgumentException("The script engine name cannot be null");
            }

            ScriptEngineConfigurator configurator = scriptEngineConfiguratorMap.get(scriptEngineName.toLowerCase());

            // Get a list of URLs from the configurator (if present), or just convert modules from Strings to URLs
            URL[] additionalClasspathURLs = null;
            if (configurator != null) {
                additionalClasspathURLs = configurator.getModuleURLsForClasspath(modules, log);
            } else {
                if (modules != null) {
                    List<URL> urls = new LinkedList<>();
                    for (String modulePathString : modules) {
                        try {
                            urls.add(new File(modulePathString).toURI().toURL());
                        } catch (MalformedURLException mue) {
                            log.error("{} is not a valid file, ignoring", new Object[]{modulePathString}, mue);
                        }
                    }
                    additionalClasspathURLs = urls.toArray(new URL[0]);
                }
            }

            // Need the right classloader when the engine is created. This ensures the NAR's execution class loader
            // (plus the module path) becomes the parent for the script engine
            ClassLoader scriptEngineModuleClassLoader = additionalClasspathURLs != null
                    ? new URLClassLoader(additionalClasspathURLs, originalContextClassLoader)
                    : originalContextClassLoader;
            if (scriptEngineModuleClassLoader != null) {
                Thread.currentThread().setContextClassLoader(scriptEngineModuleClassLoader);
            }

            for (int i = 0; i < numberOfScriptEngines; i++) {
                ScriptEngine scriptEngine = createScriptEngine();
                try {
                    if (configurator != null) {
                        configurator.init(scriptEngine, modules);
                    }
                    if (!engineQ.offer(scriptEngine)) {
                        log.error("Error adding script engine {}", new Object[]{scriptEngine.getFactory().getEngineName()});
                    }

                } catch (ScriptException se) {
                    log.error("Error initializing script engine configurator {}", new Object[]{scriptEngineName});
                    if (log.isDebugEnabled()) {
                        log.error("Error initializing script engine configurator", se);
                    }
                }
            }
        } finally {
            // Restore original context class loader
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    public void setupVariables(ProcessContext context) {
        String ScriptPath = null;
        try {
            ScriptPath = getScriptByContext(context);
        } catch (Exception e) {
            logger.error("setupVariables ProcessContext date[{}] e", Instant.now(), e);
        }
        setupVariablesByUtils(context.getProperty(SCRIPT_ENGINE).getValue(), ScriptPath,
                context.getProperty(ScriptingComponentUtils.SCRIPT_BODY).getValue(),
                context.getProperty(ScriptingComponentUtils.MODULES).evaluateAttributeExpressions().getValue());
    }

    public void setupVariables(ConfigurationContext context) {
        String ScriptPath = null;
        try {
            ScriptPath = getScriptByContext(context);
        } catch (Exception e) {
            logger.error("setupVariables ConfigurationContext date[{}] e", Instant.now(), e);
        }
        setupVariablesByUtils(context.getProperty(SCRIPT_ENGINE).getValue(), ScriptPath,
                context.getProperty(ScriptingComponentUtils.SCRIPT_BODY).getValue(),
                context.getProperty(ScriptingComponentUtils.MODULES).evaluateAttributeExpressions().getValue());

    }

    /**
     * setupVariablesByUtils
     *
     * @param scriptEngineName scriptEngineName
     * @param scriptPath       scriptPath
     */
    public void setupVariablesByUtils(String scriptEngineName, String scriptPath, String scriptBody, String modulePath) {
        this.scriptEngineName = scriptEngineName;
        this.scriptPath = scriptPath;
        this.scriptBody = scriptBody;
        if (!StringUtils.isEmpty(modulePath)) {
            modules = modulePath.split(",");
        } else {
            modules = new String[0];
        }
    }

    /**
     * getScriptPathBySql
     *
     * @return getScriptPathBySql
     */
    public String getScriptByContext(ProcessContext context) throws Exception {
        return getScriptPath(context.getProperty(ScriptingComponentUtils.DBCP_SERVICE),
                context.getProperty(ScriptingComponentUtils.QUERY_SCRIPTName_SQL),
                context.getProperty(ScriptingComponentUtils.SCRIPT_PROCESSOR_ID));
    }

    /**
     * getScriptPathBySql
     *
     * @return getScriptPathBySql
     */
    public String getScriptByContext(ValidationContext context) throws Exception {
        return getScriptPath(context.getProperty(ScriptingComponentUtils.DBCP_SERVICE),
                context.getProperty(ScriptingComponentUtils.QUERY_SCRIPTName_SQL),
                context.getProperty(ScriptingComponentUtils.SCRIPT_PROCESSOR_ID));
    }

    /**
     * getScriptPathBySql
     *
     * @return getScriptPathBySql
     */
    public String getScriptByContext(ConfigurationContext context) throws Exception {
        return getScriptPath(context.getProperty(ScriptingComponentUtils.DBCP_SERVICE),
                context.getProperty(ScriptingComponentUtils.QUERY_SCRIPTName_SQL),
                context.getProperty(ScriptingComponentUtils.SCRIPT_PROCESSOR_ID));
    }

    /**
     * @param property  dbcpService
     * @param property2 QUERY_SCRIPTName_SQL
     * @param property3 SCRIPT_PROCESSOR_ID
     * @return ScriptPath
     * @throws Exception Exception
     */
    private String getScriptPath(PropertyValue property, PropertyValue property2, PropertyValue property3) throws Exception {
        String ScriptPath = null;
        if (null == dbcpService) {
            dbcpService = property.asControllerService(DBCPService.class);
        }
        Connection con = dbcpService.getConnection();
        String sql = property2.getValue();
        processorId = property3.getValue();

        Statement stmt = con.createStatement();
        String sql_ok = MessageFormat.format(sql, processorId);
        logger.debug("getScriptPath date[{}] Sql[{}] ", Instant.now(), sql_ok);
        ResultSet resultSet = stmt.executeQuery(sql_ok);
        while (resultSet.next()) {
            String name = resultSet.getString(1);
            String path = resultSet.getString(2);
            if (null != name && !name.isEmpty() && null != path && !path.isEmpty()) {
                ScriptPath = path.concat(name);
            }
        }
        if (!resultSet.isClosed()) resultSet.close();
        if (!stmt.isClosed()) stmt.close();
        if (!con.isClosed()) con.close();
        logger.debug("getScriptPath date[{}] ScriptPath[{}] ", Instant.now(), ScriptPath);
        return ScriptPath;
    }


    /**
     * Provides a ScriptEngine corresponding to the currently selected script engine name.
     * ScriptEngineManager.getEngineByName() doesn't use find ScriptEngineFactory.getName(), which
     * is what we used to populate the list. So just search the list of factories until a match is
     * found, then create and return a script engine.
     *
     * @return a Script Engine corresponding to the currently specified name, or null if none is found.
     */
    protected ScriptEngine createScriptEngine() {
        //
        ScriptEngineFactory factory = scriptEngineFactoryMap.get(scriptEngineName);
        if (factory == null) {
            return null;
        }
        return factory.getScriptEngine();
    }

    public void stop() {
        if (engineQ != null) {
            engineQ.clear();
        }
    }
}
