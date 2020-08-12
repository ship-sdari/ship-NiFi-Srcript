package com.sdari.publicUtils


import com.sdari.dto.manager.NifiProcessorAttributesDTO
import com.sdari.dto.manager.NifiProcessorRoutesDTO
import com.sdari.dto.manager.NifiProcessorSubClassDTO
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.Relationship

import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This class contains variables and methods common to processors.
 */

class ProcessorComponentHelper {

    final AtomicBoolean isInitialized = new AtomicBoolean(false)
    private int processorId;
    private List<PropertyDescriptor> descriptors
    private Map<String, Relationship> relationships
    private Map parameters
    private List<NifiProcessorSubClassDTO> subClasses
    private String url = 'jdbc:mysql://10.0.16.19:3306/groovy?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useLegacyDatetimeCode=false&useSSL=false&testOnBorrow=true&validationQuery=select 1'
    private String userName = 'appuser'
    private String password = 'Qgy@815133'
    private int timeOut = 10
    private Connection con

    ProcessorComponentHelper(int id) {
        //构造处理器编号
        processorId = id
        //构造管理库连接
        loadConnection()
        //根据管理库连接查询所有结果并暂存
        //缺
    }

    void loadConnection() {
        if (con == null || con.isClosed()) {
//            Class.forName('com.mysql.jdbc.Driver').newInstance()
            DriverManager.setLoginTimeout(timeOut)
            con = DriverManager.getConnection(url, userName, password)
            con.setReadOnly(true)
        }
    }

    void releaseConnection() {
        if (con != null && !con.isClosed()) {
            con.close()
        }
    }

    List<PropertyDescriptor> getDescriptors() {
        return descriptors
    }

    void setDescriptors(List<PropertyDescriptor> descriptors) {
        this.descriptors = descriptors
    }

    Map<String, Relationship> getRelationships() {
        return relationships
    }

    void setRelationships(Map<String, Relationship> relationships) {
        this.relationships = relationships
    }

    def getParameters() {
        return this.parameters
    }

    void setParameters(parameters) {
        this.parameters = parameters
    }

    def getSubClasses() {
        return this.subClasses
    }

    void setSubClasses(subClasses) {
        this.subClasses = subClasses
    }

    void createDescriptors() {
        descriptors = []

        // descriptors.add(routes_manager_utils.SCRIPT_FILE)
        // descriptors.add(routes_manager_utils.SCRIPT_BODY)
        // descriptors.add(routes_manager_utils.MODULES)

        this.isInitialized.set(true)
    }

    void createRelationships(List<String> names) {
        relationships = [:]
        relationships.putAll(RoutesManagerUtils.createRelationshipMap(names))
    }

    void createParameters(List<NifiProcessorAttributesDTO> attributeRows) {
        parameters = [:]
        parameters.putAll(AttributesManagerUtils.createAttributesMap(attributeRows))
    }

    void createSubClasses(List<NifiProcessorSubClassDTO> subClasses) {
        setSubClasses(subClasses)
    }

    void initComponent() throws Exception {
        loadConnection()
        //闭包查询路由表
        List<NifiProcessorRoutesDTO> routesDto = null
        def selectRouteManagers = {
            def routesSelect = "SELECT * FROM `nifi_processor_route` WHERE `processor_id` = ${processorId};"
            Statement stm = con.createStatement()
            ResultSet res = stm.executeQuery(routesSelect)
            routesDto = NifiProcessorRoutesDTO.createDto(res)
            if (!res.closed) res.close()
            if (!stm.isClosed()) stm.close()
        }
        selectRouteManagers.call()
        //闭包查询属性表
        List<NifiProcessorAttributesDTO> attributesDto = null
        def selectAttributeManagers = {
            def attributesSelect = "SELECT * FROM `nifi_processor_attributes` WHERE `processor_id` = ${processorId};"
            Statement stm = con.createStatement()
            ResultSet res = stm.executeQuery(attributesSelect)
            attributesDto = NifiProcessorAttributesDTO.createDto(res)
            if (!res.closed) res.close()
            if (!stm.isClosed()) stm.close()
        }
        selectAttributeManagers.call()
        //闭包查询子脚本表
        def subClassesDto = null
        def selectSubClassManagers = {
            def subClassesSelect = "SELECT * FROM `nifi_processor_sub_class` WHERE `processor_id` = ${processorId} ORDER BY `running_order`;"
            Statement stm = con.createStatement()
            ResultSet res = stm.executeQuery(subClassesSelect)
            subClassesDto = NifiProcessorSubClassDTO.createDto(res)
            if (!res.closed) res.close()
            if (!stm.isClosed()) stm.close()
        }
        selectSubClassManagers.call()
        //获取所有路由名称并设置路由暂存
        def routeNames = []
        routesDto?.each { routeNames.add(it.getProperty('route_name')) }
        createRelationships(routeNames)
        //设置属性暂存
        createParameters(attributesDto)
        //设置子脚本暂存
        createSubClasses(subClassesDto)

        this.isInitialized.set(true)
        releaseConnection()
    }
}
