package com.sdari.publicUtils

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import lombok.Data
import org.apache.commons.io.IOUtils
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.dbcp.DBCPService
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
//    private Connection con
    private DBCPService dbcpService
    private final GroovyClassLoader classLoader
    private Map<String, Class> aClasses
    private Map<String, String> publicClassesText
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

    //相关实体脚本名称
    public final static String NifiProcessorConnectionDTO = "NifiProcessorConnectionDTO.groovy"
    public final static String NifiProcessorAttributesDTO = "NifiProcessorAttributesDTO.groovy"
    public final static String NifiProcessorManagerDTO = "NifiProcessorManagerDTO.groovy"
    public final static String NifiProcessorRoutesDTO = "NifiProcessorRoutesDTO.groovy"
    public final static String NifiProcessorSubClassDTO = "NifiProcessorSubClassDTO.groovy"
    public final static String TStreamRuleDTO = "TStreamRuleDTO.groovy"
    //公共实体工具类脚本名称
    public String AttributesManagerUtils = "AttributesManagerUtils.groovy"
    public String RoutesManagerUtils = "RoutesManagerUtils.groovy"

    ProcessorComponentHelper(int id, DBCPService dbcpService) {
        classLoader = new GroovyClassLoader()
        aClasses = [:]
        //构造处理器编号
        setProcessorId(id)
        //构造管理库连接服务
//        loadConnection(con)
        setDbcpService(dbcpService)
    }

    DBCPService getDbcpService() {
        return dbcpService
    }

    void setDbcpService(DBCPService dbcpService) {
        this.dbcpService = dbcpService
    }

    Map<String, Class> getaClasses() {
        return aClasses
    }

    void setaClasses(Map<String, Class> aClasses) {
        this.aClasses = aClasses
    }

    Map<String, String> getPublicClassesText() {
        return publicClassesText
    }

    void setPublicClassesText(Map<String, String> publicClassesText) {
        this.publicClassesText = publicClassesText
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

    Connection loadConnection() {
        Connection con = null
        if (dbcpService != null) {
            con = dbcpService.getConnection()
            con?.setReadOnly(true)
        }
        con
    }

    static void releaseConnection(Connection con, Statement stmt, ResultSet res) {
        if (res != null && !res.isClosed()) {
            res.close()
        }
        if (stmt != null && !stmt.isClosed()) {
            stmt.close()
        }
        if (con != null && !con.isClosed()) {
            con.close()
        }
    }

    void createDescriptors() {
        descriptors = [:]
        // descriptors.add(routes_manager_utils.SCRIPT_FILE)
        // descriptors.add(routes_manager_utils.SCRIPT_BODY)
        // descriptors.add(routes_manager_utils.MODULES)
        setDescriptors([:])
    }

    void createRelationships(List<String> names) throws Exception {
        if (isInitialized.get()) return //路由创建只执行一次，如果创建成功下次的内部调用将不再创建(除非nifi前端更改处理器或者nifi重启)
        relationships = [:]
        def routesManager = getClassInstanceByNameAndPath("", RoutesManagerUtils)
        setRelationships(routesManager.invokeMethod('createRelationshipMap', names) as Map<String, Relationship>)
    }

    void createParameters(List<GroovyObject> attributeRows, Map<Integer, GroovyObject> connectionDto) throws Exception {
        parameters = [:]
        def attributesManager = getClassInstanceByNameAndPath("", AttributesManagerUtils)
        setParameters(attributesManager.invokeMethod('createAttributesMap', [attributeRows, connectionDto]) as Map)
    }

    void relaseParameters() throws Exception {
        def attributesManager = getClassInstanceByNameAndPath("", AttributesManagerUtils)
        attributesManager.invokeMethod('releaseSql', getParameters())
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

    void createPublicClassesText(List<NifiProcessorPublicDTO> publicDTOList) throws Exception {
        publicClassesText = [:]
        publicDTOList?.each { publicDto ->
            try {
                if (null != publicDto.public_script_text && !(publicDto.public_script_text).isEmpty()) {
                    publicClassesText.put(publicDto.public_script_name, publicDto.public_script_text)
                } else if (null != publicDto.public_full_path) {
                    final InputStream ins = new FileInputStream(new File((publicDto.public_full_path).concat(publicDto.public_script_name)))
                    final String body = IOUtils.toString(ins, "UTF-8")
                    ins.close()
                    publicClassesText.put(publicDto.public_script_name, body)
                } else {
                    throw new Exception("公共类管理表module_id = ${publicDto.module_id} 配置有异常,请检查！")
                }
            } catch (Exception e) {
                throw e
            }
        }
    }

    void initComponent() throws Exception {
        Connection con = null
        try {
            con = loadConnection()
            //闭包查询公共类表
            List<NifiProcessorPublicDTO> publicDTOList = null
            def selectPublic = {
                Statement stm = null
                ResultSet res = null
                try {
                    def publicSelect = "SELECT * FROM `nifi_processor_public` WHERE `status` = 'A';"
                    stm = con.createStatement()
                    res = stm.executeQuery(publicSelect)
                    NifiProcessorPublicDTO publicDTO = new NifiProcessorPublicDTO()
                    publicDTOList = publicDTO.createDto(res)
                } catch (Exception e) {
                    throw new Exception("闭包查询公共类表异常", e)
                } finally {
                    releaseConnection(null, stm, res)//释放连接
                }
            }
            selectPublic.call()
            createPublicClassesText(publicDTOList)
            //闭包查询处理器表
            GroovyObject processorDto = null
            def selectProcessorManagers = {
                Statement stm = null
                ResultSet res = null
                try {
                    def processorsSelect = "SELECT * FROM `nifi_processor_manager` WHERE `processor_id` = ${processorId};"
                    stm = con.createStatement()
                    res = stm.executeQuery(processorsSelect)
                    def processorDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorManagerDTO) as GroovyObject
                    processorDto = processorDtoGroovy.invokeMethod("createDto", res) as GroovyObject
                } catch (Exception e) {
                    throw new Exception("闭包查询处理器表异常", e)
                } finally {
                    releaseConnection(null, stm, res)//释放连接
                }
            }
            selectProcessorManagers.call()
            setProcessor(processorDto)//设置处理器管理配置
            //闭包查询路由表
            List<GroovyObject> routesDto = null
            def selectRouteManagers = {
                Statement stm = null
                ResultSet res = null
                try {
                    def routesSelect = "SELECT * FROM `nifi_processor_route` WHERE `processor_id` = ${processorId};"
                    stm = con.createStatement()
                    res = stm.executeQuery(routesSelect)
                    def routesDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorRoutesDTO) as GroovyObject
                    routesDto = routesDtoGroovy.invokeMethod("createDto", res) as List<GroovyObject>
                } catch (Exception e) {
                    throw new Exception("闭包查询路由表异常", e)
                } finally {
                    releaseConnection(null, stm, res)//释放连接
                }
            }
            selectRouteManagers.call()
            //闭包查询属性表
            List<GroovyObject> attributesDto = null
            def selectAttributeManagers = {
                Statement stm = null
                ResultSet res = null
                try {
                    def attributesSelect = "SELECT * FROM `nifi_processor_attributes` WHERE `processor_id` = ${processorId};"
                    stm = con.createStatement()
                    res = stm.executeQuery(attributesSelect)
                    def attributesDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorAttributesDTO)
                    attributesDto = attributesDtoGroovy.invokeMethod("createDto", res) as List<GroovyObject>
                } catch (Exception e) {
                    throw new Exception("闭包查询属性表异常", e)
                } finally {
                    releaseConnection(null, stm, res)//释放连接
                }
            }
            selectAttributeManagers.call()
            //闭包查询数据库连接管理表
            Map<Integer, GroovyObject> connectionDto = null
            def selectConnectionManagers = {
                Statement stm = null
                ResultSet res = null
                try {
                    def attributesSelect = "SELECT * FROM `nifi_processor_connection` WHERE `status` = 'A';"
                    stm = con.createStatement()
                    res = stm.executeQuery(attributesSelect)
                    def connectionDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorConnectionDTO)
                    connectionDto = connectionDtoGroovy.invokeMethod("createDto", res) as Map<Integer, GroovyObject>
                } catch (Exception e) {
                    throw new Exception("闭包查询连接池表异常", e)
                } finally {
                    releaseConnection(null, stm, res)//释放连接
                }
            }
            selectConnectionManagers.call()
            //闭包查询子脚本表
            def subClassesDto = null
            def selectSubClassManagers = {
                Statement stm = null
                ResultSet res = null
                try {
                    def subClassesSelect = "SELECT * FROM `nifi_processor_sub_class` WHERE `processor_id` = ${processorId} ORDER BY `running_order`;"
                    stm = con.createStatement()
                    res = stm.executeQuery(subClassesSelect)
                    def subClassDtoGroovy = getClassInstanceByNameAndPath("", NifiProcessorSubClassDTO)
                    subClassesDto = subClassDtoGroovy.invokeMethod("createDto", res)
                } catch (Exception e) {
                    throw new Exception("闭包查询子脚本表异常", e)
                } finally {
                    releaseConnection(null, stm, res)//释放连接
                }
            }
            selectSubClassManagers.call()
            //闭包查询流规则配置表
            def tStreamRuleDto = null
            def selectConfigs = {
                Statement stmBasic = null
                Statement stmAlarm = null
                Statement stmCalculation = null
                Statement stmCollection = null
                Statement stmDist = null
                Statement stmShoreBased = null
                Statement stmThinning = null
                Statement stmWarehousing = null
                ResultSet resBasic = null
                ResultSet resAlarm = null
                ResultSet resCalculation = null
                ResultSet resCollection = null
                ResultSet resDist = null
                ResultSet resShoreBased = null
                ResultSet resThinning = null
                ResultSet resWarehousing = null
                try {
                    if ('A' != processor.getProperty('is_need_rules') || 'A' != processor.getProperty('status')) return
                    final int sid = processor.getProperty('sid') as int
                    def tStreamRuleSelectBasic = "SELECT * FROM `tstream_rule` WHERE `sid` = ${sid} AND `status` = 'A';"
                    def tStreamRuleSelectAlarm = "SELECT * FROM `tstream_rule_alarm` WHERE `sid` = ${sid};"
                    def tStreamRuleSelectCalculation = "SELECT * FROM `tstream_rule_calculation` WHERE `sid` = ${sid};"
                    def tStreamRuleSelectCollection = "SELECT * FROM `tstream_rule_collection` WHERE `sid` = ${sid};"
                    def tStreamRuleSelectDist = "SELECT * FROM `tstream_rule_other_distributions` WHERE `sid` = ${sid};"
                    def tStreamRuleSelectShoreBased = "SELECT * FROM `tstream_rule_shore_based_distributions` WHERE `sid` = ${sid};"
                    def tStreamRuleSelectThinning = "SELECT * FROM `tstream_rule_thinning` WHERE `sid` = ${sid};"
                    def tStreamRuleSelectWarehousing = "SELECT * FROM `tstream_rule_warehousing` WHERE `sid` = ${sid};"
                    stmBasic = con.createStatement()
                    stmAlarm = con.createStatement()
                    stmCalculation = con.createStatement()
                    stmCollection = con.createStatement()
                    stmDist = con.createStatement()
                    stmShoreBased = con.createStatement()
                    stmThinning = con.createStatement()
                    stmWarehousing = con.createStatement()
                    resBasic = stmBasic.executeQuery(tStreamRuleSelectBasic)
                    resAlarm = stmAlarm.executeQuery(tStreamRuleSelectAlarm)
                    resCalculation = stmCalculation.executeQuery(tStreamRuleSelectCalculation)
                    resCollection = stmCollection.executeQuery(tStreamRuleSelectCollection)
                    resDist = stmDist.executeQuery(tStreamRuleSelectDist)
                    resShoreBased = stmShoreBased.executeQuery(tStreamRuleSelectShoreBased)
                    resThinning = stmThinning.executeQuery(tStreamRuleSelectThinning)
                    resWarehousing = stmWarehousing.executeQuery(tStreamRuleSelectWarehousing)

                    def ruleDtoGroovy = getClassInstanceByNameAndPath("", TStreamRuleDTO)
                    tStreamRuleDto = ruleDtoGroovy.invokeMethod('createDto', [resBasic, resAlarm, resCalculation, resCollection, resDist, resShoreBased, resThinning, resWarehousing])
                } catch (Exception e) {
                    throw new Exception("闭包查询流规则配置表异常", e)
                } finally {
                    //释放连接
                    releaseConnection(null, stmBasic, resBasic)
                    releaseConnection(null, stmAlarm, resAlarm)
                    releaseConnection(null, stmCalculation, resCalculation)
                    releaseConnection(null, stmCollection, resCollection)
                    releaseConnection(null, stmDist, resDist)
                    releaseConnection(null, stmShoreBased, resShoreBased)
                    releaseConnection(null, stmThinning, resThinning)
                    releaseConnection(null, stmWarehousing, resWarehousing)
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
                createParameters(attributesDto, connectionDto)
                //设置子脚本分组并暂存
                createSubClasses(subClassesDto, routeMap)
                //设置流规则暂存
                createTStreamRules(tStreamRuleDto)
            } catch (Exception e) {
                throw new Exception("配置暂存异常", e)
            }
        } catch (Exception exception) {
            throw exception
        } finally {
            releaseConnection(con, null, null)
        }
        this.isInitialized.set(true)//初始化成功
    }

    /**
     * 暂存仓库的资源释放
     */
    void releaseComponent() throws Exception {
        relaseParameters()//关闭长连接
        parameters?.clear()
        routeConf?.clear()
        subClasses?.clear()
        scriptMap?.clear()
        tStreamRules?.clear()
        aClasses?.clear()
        publicClassesText?.clear()
        this.isInitialized.set(false)//恢复初始化状态
    }
    /**
     * 初始化子脚本并暂存至脚本实例仓库
     */
    void initScript(final ComponentLog log, final String processorName) throws Exception {
        boolean isInit = false
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
            isInit = true
        } catch (Exception e) {
            throw new Exception("初始化子脚本并暂存至脚本实例仓库异常", e)
        } finally {
            this.isInitialized.set(isInit)
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
    GroovyObject getClassInstanceByNameAndPath(String path, String name) {
        def returnInstance = null
        final String fullPath = path + name
        try {
            Class aClass
            if (aClasses.containsKey(fullPath)) {
                aClass = aClasses.get(fullPath)
            } else {
//                aClass = classLoader.parseClass(new File(fullPath))
                if (!publicClassesText.containsKey(fullPath)) {
                    throw new Exception("没有该脚本的内容供于创建！")
                }
                aClass = classLoader.parseClass(publicClassesText.get(fullPath))
                aClasses.put(fullPath, aClass)
            }
            returnInstance = aClass.newInstance() as GroovyObject
        } catch (Exception e) {
            throw new Exception("实例化脚本对象 ${fullPath} 出现异常", e)
        } finally {
            returnInstance
        }
    }

    /**
     * 深拷贝工具类(舍弃)
     */
    static def deepClone(def map) {
        String json = JSON.toJSONString(map, SerializerFeature.WriteMapNullValue)
        return JSON.parseObject(json, map.getClass() as Class<Object>)
    }

    /**
     * 深拷贝工具类
     */
    /*static <T> T deepClone(T src) throws RuntimeException {
        ByteArrayOutputStream memoryBuffer = new ByteArrayOutputStream()
        ObjectOutputStream out = null
        ObjectInputStream inp = null
        T dist = null
        try {
            out = new ObjectOutputStream(memoryBuffer)
            out.writeObject(src)
            out.flush()
            inp = new ObjectInputStream(new ByteArrayInputStream(memoryBuffer.toByteArray()))
            dist = (T) inp.readObject()
        } catch (Exception e) {
            throw new RuntimeException("反序列化深拷贝失败", e)
        } finally {
            if (out != null) {
                try {
                    out.close()
                    out = null
                } catch (IOException e) {
                    throw new RuntimeException(e)
                }
            }
            if (inp != null) {
                try {
                    inp.close()
                    inp = null
                } catch (IOException e) {
                    throw new RuntimeException(e)
                }
            }
        }
        return dist
    }*/
}

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorPublicDTO {
    private Integer module_id
    private String public_full_path
    private String public_script_name
    private String public_script_text
    private String public_script_desc
    private String status

    static List<NifiProcessorPublicDTO> createDto(ResultSet res) throws Exception {
        try {
            def NifiProcessorPublicDTO = []
            while (res.next()) {
                NifiProcessorPublicDTO dto = new NifiProcessorPublicDTO()
                dto.module_id = res.getInt('module_id')
                dto.public_full_path = res.getString('public_full_path')
                dto.public_script_name = res.getString('public_script_name')
                dto.public_script_text = res.getString('public_script_text')
                dto.public_script_desc = res.getString('public_script_desc')
                dto.status = res.getString('status')
                NifiProcessorPublicDTO.add(dto)
            }
            NifiProcessorPublicDTO
        } catch (Exception e) {
            throw new Exception("NifiProcessorPublicDTO createDto has an error", e)
        }
    }
}