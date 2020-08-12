package com.sdari.publicUtils


import com.sdari.dto.manager.NifiProcessorAttributesDTO
import com.sdari.dto.manager.NifiProcessorRoutesDTO
import com.sdari.dto.manager.NifiProcessorSubClassDTO
import com.sdari.dto.manager.TStreamRuleDTO
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
    private Map<String, Map<String, TStreamRuleDTO>> tStreamRules
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

    def getTStreamRules() {
        return this.tStreamRules
    }

    void setTStreamRules(tStreamRuleDto) {
        this.tStreamRules = tStreamRuleDto
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

    void createTStreamRules(Map<String, Map<String, TStreamRuleDTO>> tStreamRuleDto) {
        setTStreamRules(tStreamRuleDto)
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
        //闭包查询流规则配置表
        def tStreamRuleDto = null
        def selectConfigs = {
            def tStreamRuleSelectBasic = "SELECT * FROM `tstream_rule`"
            def tStreamRuleSelectAlarm = "SELECT * FROM `tstream_rule_alarm`"
            def tStreamRuleSelectCalculation = "SELECT * FROM `tstream_rule_calculation`"
            def tStreamRuleSelectCollection = "SELECT * FROM `tstream_rule_collection`"
            def tStreamRuleSelectDist = "SELECT * FROM `tstream_rule_other_distributions`"
            def tStreamRuleSelectShoreBased = "SELECT * FROM `tstream_rule_shore_based_distributions`"
            def tStreamRuleSelectThinning = "SELECT * FROM `tstream_rule_thinning`"
            def tStreamRuleSelectWarehousing = "SELECT * FROM `tstream_rule_warehousing`"
            Statement stmBasic = con.createStatement()
            Statement stmAlarm = con.createStatement()
            Statement stmCalculation = con.createStatement()
            Statement stmCollection = con.createStatement()
            Statement stmDist = con.createStatement()
            Statement stmShoreBased = con.createStatement()
            Statement stmThinning = con.createStatement()
            Statement stmWarehousing = con.createStatement()
            ResultSet resBasic = stmBasic.executeQuery(tStreamRuleSelectBasic)
            ResultSet resAlarm = stmAlarm.executeQuery(tStreamRuleSelectAlarm)
            ResultSet resCalculation = stmCalculation.executeQuery(tStreamRuleSelectCalculation)
            ResultSet resCollection = stmCollection.executeQuery(tStreamRuleSelectCollection)
            ResultSet resDist = stmDist.executeQuery(tStreamRuleSelectDist)
            ResultSet resShoreBased = stmShoreBased.executeQuery(tStreamRuleSelectShoreBased)
            ResultSet resThinning = stmThinning.executeQuery(tStreamRuleSelectThinning)
            ResultSet resWarehousing = stmWarehousing.executeQuery(tStreamRuleSelectWarehousing)
            tStreamRuleDto = TStreamRuleDTO.createDto(resBasic, resAlarm, resCalculation, resCollection, resDist, resShoreBased, resThinning, resWarehousing)
            if (!resBasic.closed) resBasic.close()
            if (!stmBasic.isClosed()) stmBasic.close()
            if (!resAlarm.closed) resAlarm.close()
            if (!stmAlarm.isClosed()) stmAlarm.close()
            if (!resCalculation.closed) resCalculation.close()
            if (!stmCalculation.isClosed()) stmCalculation.close()
            if (!resCollection.closed) resCollection.close()
            if (!stmCollection.isClosed()) stmCollection.close()
            if (!resDist.closed) resDist.close()
            if (!stmDist.isClosed()) stmDist.close()
            if (!resShoreBased.closed) resShoreBased.close()
            if (!stmShoreBased.isClosed()) stmShoreBased.close()
            if (!resThinning.closed) resThinning.close()
            if (!stmThinning.isClosed()) stmThinning.close()
            if (!resWarehousing.closed) resWarehousing.close()
            if (!stmWarehousing.isClosed()) stmWarehousing.close()
        }
        selectConfigs.call()
        //获取所有路由名称并设置路由暂存
        def routeNames = []
        routesDto?.each { routeNames.add(it.getProperty('route_name')) }
        createRelationships(routeNames)
        //设置属性暂存
        createParameters(attributesDto)
        //设置子脚本暂存
        createSubClasses(subClassesDto)
        //设置流规则暂存
        createTStreamRules(tStreamRuleDto)

        this.isInitialized.set(true)
        releaseConnection()
    }
}
