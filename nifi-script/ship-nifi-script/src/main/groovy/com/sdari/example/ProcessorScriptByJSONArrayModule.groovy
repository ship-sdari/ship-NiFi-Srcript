package com.sdari.example

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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription('')
class ProcessorScriptByJSONArrayModule implements Processor {
    static def log
    //处理器id，同处理器管理表中的主键一致，由调度处理器中的配置同步而来
    private String id
    private String currentClassName = this.class.canonicalName
    private DBCPService dbcpService = null
    private GroovyObject pch

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
        final AtomicReference<JSONArray> datas = new AtomicReference<>()
        session.read(flowFile, { inputStream ->
            try {
                datas.set(JSONArray.parseArray(IOUtils.toString(inputStream, StandardCharsets.UTF_8)))
            } catch (Exception e) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 读取流文件失败", e
                onFailure(session, flowFile)
                session.commit()
            }
        })
        try {
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
                                (pch.getProperty("returnData") as String)      : dataList]
            //循环路由名称 根据路由状态处理 [路由名称->路由实体]
            String routeName = ''
            for (routesDTO in (pch.getProperty('routeConf') as Map<String, GroovyObject>)?.values()) {
                try {
                    routeName = routesDTO.getProperty('route_name') as String
                    if ('A' == routesDTO.getProperty('route_running_way')) {
                        log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的运行方式，暂不支持并行执行方式，请检查路由管理表!"
                        continue
                    }
                    //用来接收脚本返回的数据
                    Map returnMap = pch.invokeMethod("deepClone",former) as Map
                    //路由方式 A-正常路由 I-源文本路由 S-不路由
                    def routeStatus = 'S'
                    //路由关系
                    switch (routesDTO.getProperty('status')) {
                    //路由关系禁用
                        case "S":
                            routeStatus = 'S'
                            break
                    //路由关系忽略，应当源文本路由
                        case "I":
                            routeStatus = 'I'
                            break
                    //路由关系正常执行
                        default:
                            def subClasses = (pch.getProperty('subClasses') as Map<String, Map<String, List<GroovyObject>>>)
                            //开始循环分脚本
                            if ((subClasses.get(routeName) as Map<String, List<GroovyObject>>).size() > 1) {
                                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的分脚本运行方式配置异常，请检查子脚本管理表!"
                                break
                            }
                            for (runningWay in subClasses.get(routeName).keySet()) {
                                //执行方式 A-并行 S-串行
                                if ("S" == runningWay) {
                                    for (subClassDTO in subClasses.get(routeName).get(runningWay)) {
                                        if ('A' == subClassDTO.getProperty('status')) {
                                            //根据路由名称 获取脚本实体GroovyObject instance
                                            final GroovyObject instance = pch.invokeMethod("getScriptMapByName", (subClassDTO.getProperty('sub_script_name') as String)) as GroovyObject
                                            //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                                            returnMap = (instance.invokeMethod(pch.getProperty("funName") as String, returnMap) as Map)
                                            routeStatus = 'A'
                                        }
                                    }
                                } else {
                                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的分脚本运行方式，暂不支持并行执行方式，请检查子脚本管理表!"
                                }
                            }
                    }
                    //如果脚本执行了路由下去
                    switch (routeStatus) {
                        case 'A':
                            def flowFiles = []
                            final List<JSONObject> returnDataList = (returnMap.get('data') as List<JSONObject>)
                            final List<JSONObject> returnAttributesList = (returnMap.get('attributes') as List<JSONObject>)
                            if ((null == returnDataList || null == returnAttributesList) || (returnDataList.size() != returnAttributesList.size())) {
                                throw new Exception('结果条数与属性条数不一致，请检查子脚本处理逻辑！')
                            }
                            for (int i = 0; i < returnDataList.size(); i++) {
                                FlowFile flowFileNew = session.create()
                                try {
                                    session.putAllAttributes(flowFileNew, (returnAttributesList.get(i) as Map<String, String>))
                                    //FlowFile write 数据
                                    session.write(flowFileNew, { out ->
                                        out.write(JSONObject.toJSONBytes(returnDataList.get(i),
                                                SerializerFeature.WriteMapNullValue))
                                    } as OutputStreamCallback)
                                    flowFiles.add(flowFileNew)
                                } catch (Exception e) {
                                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 创建流文件异常", e
                                    session.remove(flowFileNew)
                                }
                            }
                            if (null == relationships.get(routeName)) throw new Exception('没有该创建路由关系，请核对管理表！')
                            session.transfer(flowFiles, relationships.get(routeName))
                            break
                        case 'I':
                            if (null == relationships.get(routeName)) throw new Exception('没有该创建路由关系，请核对管理表！')
                            session.transfer(session.clone(flowFile), relationships.get(routeName))
                            break
                        default:
                            //不路由
                            break
                    }
                } catch (Exception e) {
                    log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] Route = ${routeName} 的处理过程有异常", e
                }
            }
            session.remove(flowFile)
        } catch (final Throwable t) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 的处理过程有异常", t
            onFailure(session, flowFile)
        } finally {
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
}

//脚本部署时需要放开该注释
//processor = new ProcessorScriptByJSONArrayModule()

