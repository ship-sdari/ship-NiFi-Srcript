package com.sdari.processor

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import com.sdari.dto.manager.NifiProcessorSubClassDTO
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
import org.python.antlr.ast.If

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription('岸基-解析数据包路由处理器')
class analysisDataBySid implements Processor {
    static def log
    private String id
    private DBCPService dbcpService = null
    private GroovyClassLoader loader = new GroovyClassLoader()
    private ProcessorComponentHelper pch

    @Override
    Set<Relationship> getRelationships() {
        Set<Relationship> set = new HashSet<Relationship>()
        Map<String, Relationship> relationshipMap = pch.getRelationships()
        for (String relation : relationshipMap.keySet()) {
            Relationship relationship = relationshipMap.get(relation)
            set.add(relationship)
        }
        return Collections.unmodifiableSet(set) as Set<Relationship>
    }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        List<PropertyDescriptor> descriptorList = new ArrayList<>()
        Map<String, PropertyDescriptor> descriptorMap = pch.getDescriptors()
        for (String name : descriptorMap.keySet()) {
            descriptorList.add(descriptorMap.get(name))
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
            final Object[] args = [[pch.returnRules: pch.getTStreamRules(), pch.returnAttributes: attributesMap, pch.returnData: dataList]]

            //循环路由名称 根据路由状态处理 [路由名称->路由实体]
            for (routesDTO in pch.getRouteConf()?.values()) {
                if ('A' == routesDTO.route_running_way) {
                    log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的运行方式，暂不支持并行执行方式，请检查管理表!"
                    continue
                }
                //用来接收脚本返回的数据
                Object[] returnList = args
                //用来判断有没有脚本被调用
                boolean isScripted = false
                //路由关系
                switch (routesDTO.status) {
                //路由关系禁用
                    case "S":
                        continue//下一个路由关系
                        break
                //路由关系忽略
                    case "I":
                        session.transfer(session.clone(flowFile), pch.getRelationships().get(routesDTO.route_name))
                        break
                //路由关系正常执行
                    default:
                        //开始循环分脚本
                        if (pch.getSubClasses().get(routesDTO.route_name).size() > 1) {
                            log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的分脚本运行方式配置异常，请检查管理表!"
                            break
                        }
                        for (runningWay in pch.getSubClasses().get(routesDTO.route_name).keySet()) {
                            //执行方式 A-并行 S-串行
                            if ("S" == runningWay) {
                                for (subClassDTO in pch.getSubClasses().get(routesDTO.route_name).get(runningWay)) {
                                    if ('A' == subClassDTO.status) {
                                        isScripted = true
                                        //根据路由名称 获取脚本实体GroovyObject instance
                                        final GroovyObject instance = pch.getScriptMapByName(subClassDTO.sub_script_name)
                                        //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                                        returnList = instance.invokeMethod(pch.funName, returnList)
                                    }
                                }
                            } else {
                                log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的分脚本运行方式，暂不支持并行执行方式，请检查管理表!"
                            }
                        }
                }
                //如果脚本执行了路由下去
                def flowFiles = []
                if (isScripted) {
                    for (data in returnList ?[0] ?[pch.returnData]) {
                        FlowFile flowFileNew = session.create()
                        OutputStream outputStream
                        session.putAllAttributes(flowFileNew, returnList[pch.returnAttributes] as Map<String, String>)
                        //FlowFile write 数据
                        session.write(flowFileNew, {
                            outputStream.write(JSONArray.toJSONBytes(data,
                                    SerializerFeature.WriteMapNullValue))
                        } as OutputStreamCallback)
                        outputStream.close()
                        flowFiles.add(flowFileNew)
                    }
                    session.transfer(flowFiles, pch.getRelationships().get(routesDTO.route_name))
                } else {
                    session.transfer(session.clone(flowFile), pch.getRelationships().get(routesDTO.route_name))
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
    public void scriptByInitId(pid, service) throws Exception {
        id = pid
        dbcpService = service
        pch = new ProcessorComponentHelper(id as int, dbcpService.getConnection())
    }
}


