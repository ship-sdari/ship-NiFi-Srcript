package com.sdari.publicUtils

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.Relationship
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.nifi.logging.ComponentLog

/**
 * This class contains variables and methods common to processors.
 */
class ProcessorComponentHelper {

    final AtomicBoolean isInitialized = new AtomicBoolean(false)
    private int processorId
    private GroovyObject processor
    private Map<String, PropertyDescriptor> descriptors
    private Map<String, Relationship> relationships
    private Map parameters
    private Map<String, GroovyObject> routeConf
    private Map<String, Map<String, List<GroovyObject>>> subClasses
    private Map<String, GroovyObject> scriptMap
    private Map<String, Map<String, GroovyObject>> tStreamRules
    private Connection con
    private final GroovyClassLoader classLoader
    private Map<String, Class> aClasses
    //脚本的方法名
    public final static String funName = "calculation"
    //脚本返回的数据
    public final static String returnData = "data"
    //脚本返回的属性
    public final static String returnAttributes = "attributes"
    //脚本返回的配置
    public final static String returnRules = "rules"
    //脚本处理器配置
    public final static String returnParameters = "parameters"
    //相关公共类全路径(服务器路径)
    public final static String managerDtoPath = "/home/sdari/app/nifi/share/groovy/com/sdari/dto/manager/"
    public final static String publicUtilsPath = "/home/sdari/app/nifi/share/groovy/com/sdari/publicUtils/"
/*
    //相关公共类全路径(本地绝对路径)
    public final static String managerDtoPath = "F:\\IDEA\\nifi\\ship-NiFi-Srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\dto\\manager\\"
    public final static String publicUtilsPath = "F:\\IDEA\\nifi\\ship-NiFi-Srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\publicUtils\\"
*/

    //相关实体全路径path(包括脚本名称)
    public final static String NifiProcessorAttributesDTO = "${managerDtoPath}NifiProcessorAttributesDTO.groovy"
    public final static String NifiProcessorManagerDTO = "${managerDtoPath}NifiProcessorManagerDTO.groovy"
    public final static String NifiProcessorRoutesDTO = "${managerDtoPath}NifiProcessorRoutesDTO.groovy"
    public final static String NifiProcessorSubClassDTO = "${managerDtoPath}NifiProcessorSubClassDTO.groovy"
    public final static String TStreamRuleDTO = "${managerDtoPath}TStreamRuleDTO.groovy"

    //公共实体工具类path
    public String AttributesManagerUtils = "${publicUtilsPath}AttributesManagerUtils.groovy"
    public String RoutesManagerUtils = "${publicUtilsPath}RoutesManagerUtils.groovy"

    ProcessorComponentHelper(int id, Connection con) {
        classLoader = new GroovyClassLoader()
        aClasses = [:]
        //构造处理器编号
        setProcessorId(id)
        //构造管理库连接
        loadConnection(con)
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

    Map<String, GroovyObject> getScriptMap() {
        return scriptMap
    }

    void setScriptMap(Map<String, GroovyObject> scriptMap) {
        this.scriptMap = scriptMap
    }

    int getProcessorId() {
        return processorId
    }

    void setProcessorId(int processorId) {
        this.processorId = processorId
    }

    static String getFunName() {
        return funName
    }

    static String getReturnData() {
        return returnData
    }

    static String getReturnAttributes() {
        return returnAttributes
    }

    static String getReturnRules() {
        return returnRules
    }

    static String getReturnParameters() {
        return returnParameters
    }

    GroovyObject getProcessor() {
        return this.processor
    }

    void setProcessor(GroovyObject processorManagerDTO) {
        this.processor = processorManagerDTO
    }

    Map<String, GroovyObject> getRouteConf() {
        return this.routeConf
    }

    void setRouteConf(Map<String, GroovyObject> routeConf) {
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
        setDescriptors([:])
    }

    void createRelationships(List<String> names) throws Exception {
        relationships = [:]
        def routesManager = getClassInstanceByNameAndPath("", RoutesManagerUtils)
        setRelationships(routesManager.invokeMethod('createRelationshipMap', names) as Map<String, Relationship>)
    }

    void createParameters(List<GroovyObject> attributeRows) throws Exception {
        parameters = [:]
        def attributesManager = getClassInstanceByNameAndPath("", AttributesManagerUtils)
        setParameters(attributesManager.invokeMethod('createAttributesMap', attributeRows) as Map)
    }

    void createSubClasses(List<GroovyObject> subClasses, Map<Integer, GroovyObject> routeMap) throws Exception {
        Map<String, Map<String, List<GroovyObject>>> subClassGroups = [:]
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

    void createTStreamRules(Map<String, Map<String, GroovyObject>> tStreamRuleDto) {
        setTStreamRules(tStreamRuleDto)
    }

    void initComponent() throws Exception {
        loadConnection()
        //闭包查询处理器表
        GroovyObject processorDto = null
        def selectProcessorManagers = {
            try {
                def processorsSelect = "SELECT * FROM `nifi_processor_manager` WHERE `processor_id` = ${processorId};"
                Statement stm = con.createStatement()
                ResultSet res = stm.executeQuery(processorsSelect)
                def processorDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorManagerDTO) as GroovyObject
                processorDto = processorDtoGroovy.invokeMethod("createDto", res) as GroovyObject
                if (!res.closed) res.close()
                if (!stm.isClosed()) stm.close()
            } catch (Exception e) {
                throw new Exception("闭包查询处理器表异常", e)
            }
        }
        selectProcessorManagers.call()
        setProcessor(processorDto)//设置处理器管理配置
        //闭包查询路由表
        List<GroovyObject> routesDto = null
        def selectRouteManagers = {
            try {
                def routesSelect = "SELECT * FROM `nifi_processor_route` WHERE `processor_id` = ${processorId};"
                Statement stm = con.createStatement()
                ResultSet res = stm.executeQuery(routesSelect)
                def routesDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorRoutesDTO) as GroovyObject
                routesDto = routesDtoGroovy.invokeMethod("createDto", res) as List<GroovyObject>
                if (!res.closed) res.close()
                if (!stm.isClosed()) stm.close()
            } catch (Exception e) {
                throw new Exception("闭包查询路由表异常", e)
            }
        }
        selectRouteManagers.call()
        //闭包查询属性表
        List<GroovyObject> attributesDto = null
        def selectAttributeManagers = {
            try {
                def attributesSelect = "SELECT * FROM `nifi_processor_attributes` WHERE `processor_id` = ${processorId};"
                Statement stm = con.createStatement()
                ResultSet res = stm.executeQuery(attributesSelect)

                def attributesDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorAttributesDTO)
                attributesDto = attributesDtoGroovy.invokeMethod("createDto", res) as List<GroovyObject>
                if (!res.closed) res.close()
                if (!stm.isClosed()) stm.close()
            } catch (Exception e) {
                throw new Exception("闭包查询属性表异常", e)
            }
        }
        selectAttributeManagers.call()
        //闭包查询子脚本表
        def subClassesDto = null
        def selectSubClassManagers = {
            try {
                def subClassesSelect = "SELECT * FROM `nifi_processor_sub_class` WHERE `processor_id` = ${processorId} ORDER BY `running_order`;"
                Statement stm = con.createStatement()
                ResultSet res = stm.executeQuery(subClassesSelect)
                def subClassDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorSubClassDTO)
                subClassesDto = subClassDtoGroovy.invokeMethod("createDto", res)
                if (!res.closed) res.close()
                if (!stm.isClosed()) stm.close()
            } catch (Exception e) {
                throw new Exception("闭包查询子脚本表异常", e)
            }
        }
        selectSubClassManagers.call()
        //闭包查询流规则配置表
        def tStreamRuleDto = null
        def selectConfigs = {
            try {
                if ('A' != processor.getProperty('is_need_rules') || 'A' != processor.getProperty('status')) return
                final int sid = processor.getProperty('sid') as int
                def tStreamRuleSelectBasic = "SELECT * FROM `tstream_rule` WHERE `sid` = ${sid};"
                def tStreamRuleSelectAlarm = "SELECT * FROM `tstream_rule_alarm` WHERE `sid` = ${sid};"
                def tStreamRuleSelectCalculation = "SELECT * FROM `tstream_rule_calculation` WHERE `sid` = ${sid};"
                def tStreamRuleSelectCollection = "SELECT * FROM `tstream_rule_collection` WHERE `sid` = ${sid};"
                def tStreamRuleSelectDist = "SELECT * FROM `tstream_rule_other_distributions` WHERE `sid` = ${sid};"
                def tStreamRuleSelectShoreBased = "SELECT * FROM `tstream_rule_shore_based_distributions` WHERE `sid` = ${sid};"
                def tStreamRuleSelectThinning = "SELECT * FROM `tstream_rule_thinning` WHERE `sid` = ${sid};"
                def tStreamRuleSelectWarehousing = "SELECT * FROM `tstream_rule_warehousing` WHERE `sid` = ${sid};"
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

                def ruleDtoGroovy = getClassInstanceByNameAndPath("", TStreamRuleDTO)
                tStreamRuleDto = ruleDtoGroovy.invokeMethod('createDto', [resBasic, resAlarm, resCalculation, resCollection, resDist, resShoreBased, resThinning, resWarehousing])
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
            } catch (Exception e) {
                throw new Exception("闭包查询流规则配置表异常", e)
            }
        }
        selectConfigs.call()
        try {
            //获取所有路由名称并设置路由暂存,并暂存路由配置
            Map<String, GroovyObject> routeConf = [:]
            Map<Integer, GroovyObject> routeMap = [:]
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
        } catch (Exception e) {
            throw new Exception("配置暂存异常", e)
        }
        releaseConnection()
        this.isInitialized.set(true)
    }
    /**
     * 初始化子脚本并暂存至脚本实例仓库
     */
    void initScript(final ComponentLog log, final String processorName) throws Exception {
        try {
            Map<String, GroovyObject> GroovyObjectMap = new HashMap<>()
            for (classDTOMap in subClasses.values()) {
                for (classDTOList in classDTOMap.values()) {
                    for (classDTO in classDTOList) {
                        if ("A" == classDTO.getProperty('status') && !GroovyObjectMap.containsKey(classDTO.getProperty('sub_script_name'))) {
                            Class aClass = null
                            if ((null == classDTO.getProperty('sub_script_text') || (classDTO.getProperty('sub_script_text') as String).isEmpty())
                                    && (null != classDTO.getProperty('sub_full_path'))) {
                                def path = classDTO.getProperty('sub_full_path') + classDTO.getProperty('sub_script_name')
                                aClass = classLoader.parseClass(new File(path))
                            } else if (null != classDTO.getProperty('sub_script_text') && !(classDTO.getProperty('sub_script_text') as String).isEmpty()) {
                                def path = classDTO.getProperty('sub_script_text') as String
                                aClass = classLoader.parseClass(path)
                            } else {
                                throw new Exception("无法定位 route_id = ${classDTO.getProperty('route_id')} 的子脚本！")
                            }
                            GroovyObjectMap.put(classDTO.getProperty('sub_script_name') as String, aClass?.newInstance(log, processorId, processorName, classDTO.getProperty('route_id')) as GroovyObject)
                        }
                    }
                }
            }
            setScriptMap(GroovyObjectMap)
        } catch (Exception e) {
            this.isInitialized.set(false)
            throw new Exception("初始化子脚本并暂存至脚本实例仓库异常", e)
        }
    }

    /**
     * 将流文件无用的属性删掉掉
     */
    static Map<String, String> updateAttributes(Map<String, String> attributes) throws Exception {
        Map<String, String> map = new HashMap<>()
        try {
            if (null == attributes || attributes.size() < 1) return attributes
            for (key in attributes.keySet()) {
                switch (key) {
                    case "filename":
                        break
                    case "path":
                        break
                    case "uuid":
                        break
                    default:
                        map.put(key, attributes.get(key))
                }
            }
        } catch (Exception e) {
            throw new Exception("流文件属性更新异常", e)
        }
        map
    }

    /**
     * groovy实例化外部脚本类的获取
     */
    GroovyObject getClassInstanceByNameAndPath(String name, String path) {
        def returnInstance = null
        final String fullPath = path + name
        try {
            final Class aClass
            if (aClasses.containsKey(fullPath)) {
                aClass = aClasses.get(fullPath)
            } else {
                aClass = classLoader.parseClass(new File(fullPath))
                aClasses.put(fullPath, aClass)
            }
            returnInstance = aClass.newInstance() as GroovyObject
        } catch (Exception e) {
            throw new Exception("实例化脚本对象 ${fullPath} 出现异常", e)
        } finally {
            returnInstance
        }
    }
}