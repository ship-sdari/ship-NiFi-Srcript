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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Utility methods and constants used by the scripting components.
 */
public class ScriptingComponentUtils {
    /**
     * A relationship indicating flow files were processed successfully
     */
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that were successfully processed")
            .build();

    /**
     * A relationship indicating an error while processing flow files
     */
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be processed")
            .build();

    /**
     * A property descriptor for specifying the location of a script file
     */
    public static final PropertyDescriptor SCRIPT_FILE = new PropertyDescriptor.Builder()
            .name("Script File")
            .required(false)
            .description("Path to script file to execute. Only one of Script File or Script Body may be used")
            .addValidator(new StandardValidators.FileExistsValidator(true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    /**
     * A property descriptor for specifying the body of a script
     */
    public static final PropertyDescriptor SCRIPT_BODY = new PropertyDescriptor.Builder()
            .name("Script Body")
            .required(false)
            .description("Body of script to execute. Only one of Script File or Script Body may be used")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    /**
     * A property descriptor for specifying the location of additional modules to be used by the script
     */
    public static final PropertyDescriptor MODULES = new PropertyDescriptor.Builder()
            .name("Module Directory")
            .description("Comma-separated list of paths to files and/or directories which contain modules required by the script.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("DBCP_SERVICE")
            .description("The Controller Service that is used to obtain connection to database(用于获取到数据库的连接的控制器服务)")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();
    public static final PropertyDescriptor QUERY_SCRIPTName_SQL = new PropertyDescriptor.Builder()
            .name("QUERY_SCRIPT_SQL")
            .required(false)
            .description("获取脚本全路径及脚本名称")
            .defaultValue("SELECT `script_name`,`full_path` ,`script_text` FROM `nifi_processor_manager` where  `status`=''A'' and`processor_id` = ''{0}'' ;")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor SCRIPT_PROCESSOR_ID = new PropertyDescriptor.Builder()
            .name("SCRIPT_PROCESSOR_ID")
            .required(true)
            .description("脚本processor_id")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(null)
            .build();
}

