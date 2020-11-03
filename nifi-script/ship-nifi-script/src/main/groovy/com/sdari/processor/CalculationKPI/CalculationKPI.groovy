package com.sdari.processor.CalculationKPI

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.annotation.lifecycle.OnStopped
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.*
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Statement
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription('船基-指标计算处理器')
class CalculationKPI implements Processor {
    static def log
    //处理器id，同处理器管理表中的主键一致，由调度处理器中的配置同步而来
    private String id
    private String currentClassName = this.class.canonicalName
    private DBCPService dbcpService = null
    private GroovyObject pch
    //入库相关
    final static String databaseName = 'database.name'
    final static String tableName = 'table.name'
    final static String option = 'option'
    //计算表名
    final static String tableNameByKpi = 't_calculation'
    final static String kpiRoutesName = 'kpi'
    //船位相关
    final static String tableNameByShipPosition = 't_ship_position'
    final static String ShipPositionRoutesName = 'ship_position'
    final static String latitude = 'latitude'
    final static String longitude = 'longitude'

    //处理器数据库连接 相关参数
    private Connection con
    private String urls
    private String driverByClass
    private String userName
    private String password
    private String sql
    private String databaseNameByData
    //船舶配置 sdi-><key->value>
    private Map<String, Map<String, String>> shipConf = null

    @Override
    Set<Relationship> getRelationships() {
        Set<Relationship> set = new HashSet<Relationship>()
        Map<String, Relationship> relationshipMap = (pch?.invokeMethod("getRelationships", null) as Map<String, Relationship>)
        if (relationshipMap != null && relationshipMap.size() > 0) {
            for (String relation : relationshipMap.keySet()) {
                Relationship relationship = relationshipMap.get(relation)
                set.add(relationship)
            }
        }
        Collections.unmodifiableSet(set) as Set<Relationship>
    }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        List<PropertyDescriptor> descriptorList = new ArrayList<>()
        Map<String, PropertyDescriptor> descriptorMap = (pch?.invokeMethod("getDescriptors", null) as Map<String, PropertyDescriptor>)
        if (descriptorMap != null && descriptorMap.size() > 0) {
            for (String name : descriptorMap.keySet()) {
                descriptorList.add(descriptorMap.get(name))
            }
        }
        Collections.unmodifiableList(descriptorList) as List<PropertyDescriptor>
    }

    /**
     * 实现自定义处理器的不可缺方法
     * @param context 上下文
     */
    void initialize(ProcessorInitializationContext context) {

    }
    /**
     * A method that executes only once when initialized
     *
     * @param context
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            pch.invokeMethod("initComponent", null)//相关公共配置实例更新查询
            pch.invokeMethod("initScript", [log, currentClassName])
            Map confMap = pch.getProperty('parameters') as Map
            initConf(confMap)//初始化连接配置
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行异常", e
        }
    }

    /**
     * A method that executes only once when stop
     *
     * @param context
     */
    @OnStopped
    public void onStopped(final ProcessContext context) {
        try {
            pch.invokeMethod("releaseComponent", null)//相关公共配置实例清空
            if (null != con && con.isClosed()) {
                con.isClosed().clone()
            }
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器停止运行完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器停止运行异常", e
        }
    }

    /**
     * 详细处理模块
     * @param context
     * @param sessionFactory
     * @throws org.apache.nifi.processor.exception.ProcessException
     */
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession()
        FlowFile flowFile = session.get()
        if (flowFile == null) {
            session.commit()
            return
        }
        if (!(pch?.getProperty('isInitialized') as AtomicBoolean)?.get() || 'A' != (pch?.getProperty('processor') as GroovyObject)?.getProperty('status')) {
            //工具类初始化有异常或者该处理管理表处于不是开启状态就删除流文件不做任何处理
            session.remove(flowFile)
            session.commit()
            return
        }
        /*以下为正常处理数据文件的部分*/
        final AtomicReference<JSONObject> datas = new AtomicReference<>()
        session.read(flowFile, { inputStream ->
            try {
                datas.set(JSONObject.parseObject(IOUtils.toString(inputStream, StandardCharsets.UTF_8)))
            } catch (Exception e) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 读取流文件失败", e
                onFailure(session, flowFile)
                session.commit()
            }
        })
        final boolean isMinute = Instant.now().getEpochSecond() % 60 == 0
        try {
            //配置第一次初始化
            if (null == shipConf) {
                def logs = log
                transaction(logs)
            }
            if (null == datas.get() || datas.get().size() == 0) {
                throw new Exception("[Processor_id = ${id} Processor_name = ${currentClassName}] 的接收的数据为空!")
            }
            def relationships = pch.invokeMethod("getRelationships", null) as Map<String, Relationship>
            final def attributesMap = pch.invokeMethod("updateAttributes", [flowFile.getAttributes()]) as Map<String, String>
            //调用脚本需要传的参数[attributesMap-> flowFile属性][data -> flowFile数据]
            def attributesList = []
            def dataList = []
            switch (datas.get().getClass().canonicalName) {
                case 'com.alibaba.fastjson.JSONObject':
                    attributesList.add(attributesMap.clone())
                    dataList.add(datas.get())
                    break
                case 'com.alibaba.fastjson.JSONArray':
                    datas.get().each { o -> attributesList.add(attributesMap.clone()) }
                    dataList = datas.get()
                    break
                default:
                    throw new Exception("暂不支持处理当前所接收的数据类型：${datas.get().getClass().canonicalName}")
            }
            final def former = [(pch.getProperty("returnRules") as String)     : pch.getProperty('tStreamRules') as Map<String, Map<String, GroovyObject>>,
                                (pch.getProperty("returnAttributes") as String): attributesList,
                                (pch.getProperty("returnParameters") as String): pch.getProperty('parameters') as Map,
                                (pch.getProperty("returnData") as String)      : dataList,
                                'shipConf'                                     : shipConf]
            final Map routeConf = pch.getProperty('routeConf') as Map<String, GroovyObject>
            //根据路由名称 获取路由运行方式
            def routesDTO = routeConf.get(kpiRoutesName)
            String routeName = routesDTO.getProperty('route_name') as String
            if ('A' == routesDTO.getProperty('route_running_way')) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的运行方式，暂不支持并行执行方式，请检查路由管理表!"
            }
            //用来接收脚本返回的数据
            Map returnMap = pch.invokeMethod("deepClone", former) as Map
            //路由关系
            def subClasses = (pch.getProperty('subClasses') as Map<String, Map<String, List<GroovyObject>>>)
            //用来接收计算指标 返回的数据
            List<JSONArray> kpiLists = new ArrayList<>()
            //用来接收 经度纬度 数据
            List<JSONArray> longitudeLists = new ArrayList<>()
            //开始循环分脚本
            for (runningWay in subClasses.get(routeName).keySet()) {
                //执行方式 A-并行 S-串行
                if ("S" == runningWay) {
                    for (subClassDTO in subClasses.get(routeName).get(runningWay)) {
                        if ('A' == subClassDTO.getProperty('status')) {
                            //根据路由名称 获取脚本实体GroovyObject instance
                            final GroovyObject instance = pch.invokeMethod("getScriptMapByName", (subClassDTO.getProperty('sub_script_name') as String)) as GroovyObject
                            //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                            returnMap.put("con", con)

                            switch (routeName) {
                                case kpiRoutesName:
                                    Map returnMat = (instance.invokeMethod(pch.getProperty("funName") as String, returnMap) as Map)
                                    kpiLists.add(returnMat.get('data') as JSONArray)
                                    break
                                case ShipPositionRoutesName:
                                    Map returnMat = (instance.invokeMethod(pch.getProperty("funName") as String, returnMap) as Map)
                                    longitudeLists.add(returnMat.get('data') as JSONArray)
                                    break
                                default:
                                    //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                                    returnMap = (instance.invokeMethod(pch.getProperty("funName") as String, returnMap) as Map)
                            }
                        }
                    }
                } else {
                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的分脚本运行方式，暂不支持并行执行方式，请检查子脚本管理表!"
                }
            }


            //循环路由
            final List<JSONObject> returnDataList = (returnMap.get('data') as List<JSONObject>)
            final List<JSONObject> returnAttributesList = former.get('attributes') as List<JSONObject>
            for (routes in (pch.getProperty('routeConf') as Map<String, GroovyObject>)?.values()) {
                String rouName = routes.getProperty('route_name')
                try {
                    //路由关系
                    switch (routes.getProperty('status')) {
                    //路由关系禁用
                        case "S":
                            break
                    //路由关系忽略，应当源文本路由
                        case "I":
                            if (null == relationships.get(rouName)) throw new Exception('没有该创建路由关系，请核对管理表！')
                            session.transfer(session.clone(flowFile), relationships.get(rouName))
                            break
                    //路由关系正常执行
                        default:
                            if (null == relationships.get(rouName)) throw new Exception('没有该创建路由关系，请核对管理表！')
                            //如果有数据 路由下去
                            def flowFiles = []
                            for (int i = 0; i < returnAttributesList.size(); i++) {
                                FlowFile flowFileNew = session.create()
                                JSONObject ruData = new JSONObject()
                                Map<String, String> attributesMaps = returnAttributesList.get(i) as Map<String, String>
                                try {
                                    //put 入库的名称
                                    attributesMaps.put(databaseName, databaseNameByData)
                                    switch (rouName) {
                                    //计算指标处理
                                        case kpiRoutesName:
                                            //循环 返回指标的计算结果
                                            if (kpiLists.size() > 0) {
                                                for (data in kpiLists) {
                                                    //根据下标 获取对应的 计算指标数据
                                                    JSONObject mas = data.get(i) as JSONObject
                                                    ruData.putAll(mas)
                                                }
                                            }
                                            attributesMaps.put(tableName, tableNameByKpi)
                                            attributesMaps.put(option, '0')
                                            break
                                    //船位处理
                                        case ShipPositionRoutesName:
                                            if (longitudeLists.size() > 0) {
                                                for (data in longitudeLists) {
                                                    //根据下标 获取对应的 计算指标数据
                                                    JSONObject mas = data.get(i) as JSONObject
                                                    ruData.putAll(mas)
                                                }
                                                if (!ruData.containsKey(latitude) || ruData.get(latitude) == null) {
                                                    ruData = null
                                                }
                                                if (!ruData.containsKey(longitude) || ruData.get(longitude) == null) {
                                                    ruData = null
                                                }
                                            }
                                            attributesMaps.put(tableName, tableNameByShipPosition)
                                            attributesMaps.put(option, '0')
                                            break
                                        default:
                                            ruData = returnDataList.get(i)
                                    }

                                    if (ruData != null && ruData.size() > 0) {
                                        //FlowFile write 数据
                                        session.putAllAttributes(flowFileNew, attributesMaps)
                                        session.write(flowFileNew, { out ->
                                            out.write(JSONObject.toJSONBytes(ruData, SerializerFeature.WriteMapNullValue))
                                        } as OutputStreamCallback)
                                        flowFiles.add(flowFileNew)
                                    } else {
                                        session.remove(flowFileNew)
                                    }
                                } catch (Exception e) {
                                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${rouName} 创建流文件异常", e
                                    session.remove(flowFileNew)
                                }
                            }
                            if (flowFiles.size() > 0) {
                                session.transfer(flowFiles, relationships.get(rouName))
                            }
                    }
                } catch (Exception e) {
                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${rouName} 的路由程有异常", e
                }
            }
            session.remove(flowFile)
        } catch (final Throwable t) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 的处理过程有异常", t
            onFailure(session, flowFile)
        } finally {
            //整分钟更新配置
            if (isMinute) {
                def logs = log
                transaction(logs)
            }
            session.commit()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        return (pch.getProperty('descriptors') as Map<String, PropertyDescriptor>).get(name)
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    String getIdentifier() { null }

    /**
     * 失败路由处理
     * @param session
     * @param flowFile
     */
    private void onFailure(final ProcessSession session, final FlowFile flowFile) {
        session.transfer(flowFile, (pch.getProperty('relationships') as Map<String, Relationship>).get('failure'))
    }
    /**
     * 任务功能处理器最开始的同步和初始化调用方法，由调度处理器调用
     * 同步 脚本id及dbcpService
     * 实例化公共工具类
     * @param pid 处理器id
     * @param service 数据库连接的控制服务对象
     * @param processorComponentHelperText 总管理工具类脚本内容
     * @throws Exception
     */
    void scriptByInitId(pid, service, processorComponentHelperText) throws Exception {
        id = pid //同步处理器id
        try {
            dbcpService = service
            //工具类实例化
            GroovyClassLoader classLoader = new GroovyClassLoader()
            Class aClass = classLoader.parseClass(processorComponentHelperText as String)
            pch = aClass.newInstance(pid as int, service) as GroovyObject//有参构造
            pch.invokeMethod("initComponent", null)//相关公共配置实例查询
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 任务功能处理器最开始的同步和初始化调用正常结束！"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 任务功能处理器最开始的同步和初始化调用方法异常", e
        }
    }

    /**
     * 设置该处理器的logger
     * @param logger
     * @throws Exception
     */
    void setLogger(final ComponentLog logger) {
        try {
            log = logger
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] setLogger 执行成功，日志已设置完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 设置日志的调用方法异常", e
        }
    }
    /**
     * 初始化所有连接配置
     */
    void initConf(Map<String, String> confMap) throws Exception {
        databaseNameByData = confMap.get(databaseName)
        userName = confMap.get("user.name")
        password = confMap.get("password")
        sql = confMap.get("sql.conf")
        urls = confMap.get("url")
        driverByClass = confMap.get("driver.class")
    }

    /**
     * 创建连接
     */
    void ConnectionsInIt() throws Exception {
        if (null == con || con.isClosed()) {
            Class.forName(driverByClass).newInstance()
            con = DriverManager.getConnection(urls, userName, password)
        }
    }

    /**
     * 查询船配置
     */
    synchronized void transaction(def logs) {
        //如果没有库的连,或者连接断开 就新建一个连接
        if (null == con || con.isClosed()) {
            ConnectionsInIt()
        }
        Map<String, Map<String, String>> configMap = new HashMap<>()
        try {
            Statement stm = con.createStatement()
            //查询指标配置
            ResultSet res = stm.executeQuery(sql)
            //获取所有配置
            while (res.next()) {
                int sid = res.getInt(1)
                final String key = res.getString(2)
                final String value = res.getString(3)
                final String id = String.valueOf(sid)
                if (configMap.containsKey(id)) {
                    Map<String, String> map = configMap.get(id)
                    map.put(key, value)
                } else {
                    Map<String, String> map = new HashMap<>()
                    map.put(key, value)
                    configMap.put(id, map)
                }
            }
            if (!res.isClosed()) res.close()
            if (!stm.isClosed()) stm.close()
        } catch (Exception e) {
            logs.error "[Processor_id = ${id} Processor_name = ${currentClassName}] error [${e}]"
        } finally {
            shipConf = configMap
            String t = JSONObject.toJSONString(configMap)
            logs.debug "[Processor_id = ${id} Processor_name = ${currentClassName}] conf:[${t}]"
        }
    }

}

//脚本部署时需要放开该注释
//processor = new CalculationKPI()
