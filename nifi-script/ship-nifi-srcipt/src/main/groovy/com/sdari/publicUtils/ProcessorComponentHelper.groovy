package com.sdari.publicUtils


import com.sdari.dto.manager.NifiProcessorAttributesDTO
import com.sdari.dto.manager.NifiProcessorRoutesDTO
import com.sdari.dto.manager.NifiProcessorSubClassDTO
import com.sdari.dto.manager.TStreamRuleDTO
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.Relationship
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This class contains variables and methods common to processors.
 */

class ProcessorComponentHelper {

    final AtomicBoolean isInitialized = new AtomicBoolean(false)
    private int processorId
    private Map<String, PropertyDescriptor> descriptors
    private Map<String, Relationship> relationships
    private Map parameters
    private Map<String, NifiProcessorRoutesDTO> routeConf
    private Map<String, Map<String, List<NifiProcessorSubClassDTO>>> subClasses
    private Map<String, GroovyObject> scriptMap
    private Map<String, Map<String, TStreamRuleDTO>> tStreamRules
//    private String url = 'jdbc:mysql://10.0.16.19:3306/groovy?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useLegacyDatetimeCode=false&useSSL=false&testOnBorrow=true&validationQuery=select 1'
//    private String userName = 'appuser'
//    private String password = 'Qgy@815133'
//    private int timeOut = 10
    private Connection con
    //脚本的方法名
    public final static String funName = "calculation"
    //脚本返回的数据
    public final static String funData = "calculation"
    //脚本返回的属性
    public final static String funAttributes = "calculation"

    ProcessorComponentHelper(int id, Connection con) {
        //构造处理器编号
        processorId = id
        //构造管理库连接
        loadConnection(con)
        //根据管理库连接查询所有结果并暂存
        //缺
    }

    void loadConnection(Connection con) {
        if ((this.con == null || this.con.isClosed()) && (con != null && !con.isClosed())) {
//            Class.forName('com.mysql.jdbc.Driver').newInstance()
//            DriverManager.setLoginTimeout(timeOut)
//            con = DriverManager.getConnection(url, userName, password)
            this.con = con
            con.setReadOnly(true)
        }
    }

    void releaseConnection() {
        if (con != null && !con.isClosed()) {
            con.close()
        }
    }

    Map<String, NifiProcessorRoutesDTO> getRouteConf() {
        return this.routeConf
    }

    void setRouteConf(Map<String, NifiProcessorRoutesDTO> routeConf) {
        this.routeConf = routeConf
    }

    Map<String, PropertyDescriptor> getDescriptors() {
        return descriptors
    }

    void setDescriptors(Map<String, PropertyDescriptor> descriptors) {
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

    def getScriptMapByName(String name) {
        return this.scriptMap.get(name)
    }

    void setSubClasses(subClassGroups) {
        this.subClasses = subClassGroups
    }

    def getTStreamRules() {
        return this.tStreamRules
    }

    void setTStreamRules(tStreamRuleDto) {
        this.tStreamRules = tStreamRuleDto
    }

    void createDescriptors() {
        descriptors = [:]

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

    void createSubClasses(List<NifiProcessorSubClassDTO> subClasses, Map<Integer, NifiProcessorRoutesDTO> routeMap) {
        Map<String, Map<String, List<NifiProcessorSubClassDTO>>> subClassGroups = [:]
        subClasses?.each {
            final int route_id = it.getProperty('route_id') as int
            final String route_name = routeMap.get(route_id).getProperty('route_name')
            final String sub_running_way = it.getProperty('sub_running_way')
            if (!subClassGroups.containsKey(route_name)) subClassGroups.put(route_name, [:])
            if (!subClassGroups.get(route_name).containsKey(sub_running_way)) subClassGroups.get(route_name).put(sub_running_way, [])
            subClassGroups.get(route_name).get(sub_running_way).add(it)
        }
        setSubClasses(subClassGroups)
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
        //获取所有路由名称并设置路由暂存,并暂存路由配置
        Map<String, NifiProcessorRoutesDTO> routeConf = [:]
        Map<Integer, NifiProcessorRoutesDTO> routeMap = [:]
        routesDto?.each {
            routeConf.put(it.getProperty('route_name') as String, it)
            routeMap.put(it.getProperty('route_id') as int, it)
        }
        createRelationships(routeConf.keySet() as List<String>)
        setRouteConf(routeConf)//路由表配置
        //设置属性暂存
        createParameters(attributesDto)
        //设置子脚本分组并暂存
        createSubClasses(subClassesDto, routeMap)
        //设置流规则暂存
        createTStreamRules(tStreamRuleDto)

        this.isInitialized.set(true)
        releaseConnection()
    }
    /**
     * 初始化子脚本并暂存至脚本实例仓库
     */
    void initScript() throws Exception {
        Map<String, GroovyObject> GroovyObjectMap = new HashMap<>()
        for (classDTOMap in subClasses.values()) {
            for (classDTOList in classDTOMap.values()) {
                for (classDTO in classDTOList) {
                    if ("A" == classDTO.status && !GroovyObjectMap.containsKey(classDTO.sub_script_name)) {
                        def path = classDTO.sub_full_path + classDTO.sub_script_name
                        GroovyClassLoader loader = new GroovyClassLoader()
                        Class aClass = loader.parseClass(new File(path))
                        GroovyObjectMap.put(classDTO.sub_script_name, aClass.newInstance() as GroovyObject)
                    }

                }
            }
        }
        scriptMap.putAll(GroovyObjectMap)
    }
}
