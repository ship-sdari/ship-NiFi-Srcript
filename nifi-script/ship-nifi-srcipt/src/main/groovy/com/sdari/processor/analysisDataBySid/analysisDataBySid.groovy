package com.sdari.processor.analysisDataBySid

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.serializer.SerializerFeature
import com.sdari.publicUtils.ProcessorComponentHelper
import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessSessionFactory
import org.apache.nifi.processor.Processor
import org.apache.nifi.processor.ProcessorInitializationContext
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.OutputStreamCallback
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription('岸基-解析数据包路由处理器')
class analysisDataBySid implements Processor {
    static def log
    private String id
    private DBCPService dbcpService = null
    private ProcessorComponentHelper pch

    @Override
    Set<Relationship> getRelationships() {
        Set<Relationship> set = new HashSet<Relationship>()
        Map<String, Relationship> relationshipMap = pch.getRelationships()
        if (relationshipMap != null && relationshipMap.size() > 0) {
            for (String relation : relationshipMap.keySet()) {
                Relationship relationship = relationshipMap.get(relation)
                set.add(relationship)
            }
        }
        return Collections.unmodifiableSet(set) as Set<Relationship>
    }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        List<PropertyDescriptor> descriptorList = new ArrayList<>()
        Map<String, PropertyDescriptor> descriptorMap = pch.getDescriptors()
        if (descriptorMap != null && descriptorMap.size() > 0) {
            for (String name : descriptorMap.keySet()) {
                descriptorList.add(descriptorMap.get(name))
            }
        }
        Collections.unmodifiableList(descriptorList) as List<PropertyDescriptor>
    }


    /**
     * 获取logger
     * @param logger
     * @throws Exception
     */
    public static void setLogger(final ComponentLog logger) throws Exception {
        log = logger
        log.info("进去方法setLogger")
    }

    void initialize(ProcessorInitializationContext context) {

    }
    /**
     * A method that executes only once when initialized
     *
     * @param context
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        pch.initScript()
    }

    /**
     * 详细处理模块
     * @param context
     * @param sessionFactory
     * @throws org.apache.nifi.processor.exception.ProcessException
     */
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession()
        final AtomicReference<JSONArray> dataList = new AtomicReference<>()
        FlowFile flowFile = session.get()
        if (!flowFile) return
        /*以下为正常处理数据文件的部分*/
        session.read(flowFile, { inputStream ->
            try {
                dataList.set(JSONArray.parseArray(IOUtils.toString(inputStream, StandardCharsets.UTF_8)))
            } catch (Exception e) {
                onFailure(session, flowFile)
                log.error("Failed to read from flowFile", e)
                return
            }
        })
        try {
            final def attributesMap = flowFile.getAttributes()
            //调用脚本需要传的参数[attributesMap-> flowFile属性][dataList -> flowFile数据]
            final def former = ["rules"     : pch.getTStreamRules(),
                                "attributes": attributesMap,
                                "data"      : dataList.get()]
            //循环路由名称 根据路由状态处理 [路由名称->路由实体]
            for (routesDTO in pch.getRouteConf()?.values()) {
                log.info "路由循环"
                if ('A' == routesDTO.route_running_way) {
                    log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的运行方式，暂不支持并行执行方式，请检查管理表!"
                    continue
                }
                //用来接收脚本返回的数据
                def returnList = former
                //路由方式 A-正常路由 I-源文本路由 S-不路由
                def routeWay = 'S'
                //路由关系
                switch (routesDTO.status) {
                //路由关系禁用
                    case "S":
                        routeWay = 'S'
                        break
                //路由关系忽略，应当源文本路由
                    case "I":
                        routeWay = 'I'
                        break
                //路由关系正常执行
                    default:
                        //开始循环分脚本
                        if (pch.getSubClasses().get(routesDTO.route_name).size() > 1) {
                            log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的分脚本运行方式配置异常，请检查管理表!"
                            break
                        }
                        for (runningWay in pch.getSubClasses().get(routesDTO.route_name).keySet()) {
                            log.info "脚本循环"
                            //执行方式 A-并行 S-串行
                            if ("S" == runningWay) {
                                for (subClassDTO in pch.getSubClasses().get(routesDTO.route_name).get(runningWay)) {
                                    log.info "脚本明细循环"
                                    if ('A' == subClassDTO.status) {
                                        //根据路由名称 获取脚本实体GroovyObject instance
                                        final GroovyObject instance = pch.getScriptMapByName(subClassDTO.sub_script_name)
                                        //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                                        returnList = instance.invokeMethod(pch.funName, [returnList])
                                        routeWay = 'A'
                                    }
                                }
                            } else {
                                log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的分脚本运行方式，暂不支持并行执行方式，请检查管理表!"
                            }
                        }
                }
                //如果脚本执行了路由下去
                switch (routeWay) {
                    case 'A':
                        def flowFiles = []
                        for (data in returnList[pch.returnData]) {
                            try {
                                FlowFile flowFileNew = session.create()
//                                session.putAllAttributes(flowFileNew, (returnList[pch.returnAttributes] as Map<String, String>))
                                //FlowFile write 数据
                                log.info '开始写数据'
                                session.write(flowFileNew, { out ->
                                    out.write(JSONArray.toJSONBytes(data,
                                            SerializerFeature.WriteMapNullValue))
                                } as OutputStreamCallback)
                                log.info '放入流文件列表'
                                flowFiles.add(flowFileNew)
                            } catch (Exception e) {
                                log.error '处理器' + id + "创建流文件异常", e
                            }
                        }
                        log.info '进行路由操作'
                        session.transfer(flowFiles, pch.getRelationships().get(routesDTO.route_name))
                        break
                    case 'I':
                        session.transfer(session.clone(flowFile), pch.getRelationships().get(routesDTO.route_name))
                        break
                    default:
                        //不路由
                        break
                }
            }
            session.remove(flowFile)
        } catch (final Throwable t) {
            log.error('{} failed to process due to {}', [this, t] as Object[])
            onFailure(session, flowFile)
        } finally {
            session.commit()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        return pch.getDescriptors().get(name)
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    String getIdentifier() { null }

    /**
     * 失败处理
     * @param session
     * @param flowFile
     */
    private void onFailure(final ProcessSession session, final FlowFile flowFile) {
        session.transfer(flowFile, pch.getRelationships().get('failure'))
    }
    /**
     * 获取 脚本id及dbcpService
     * @param pid
     * @param service
     * @throws Exception
     */
    void scriptByInitId(pid, service) throws Exception {
        id = pid
        dbcpService = service
        try {
            pch = new ProcessorComponentHelper(pid as int, service.getConnection())
            pch.initComponent()
        } catch (Exception e) {
            log.error("InitByDto 异常", e)
        }

        log.info(" 初始化结果")
    }
}


