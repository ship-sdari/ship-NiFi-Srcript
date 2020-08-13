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
            //根据路由关系 获取对应脚本 [路由名称->脚本执行顺序（串行||并行）]
            //[attributesMap-> flowFile属性][dataMap -> flowFile数据]
            final def attributesMap = flowFile.getAttributes()
            Object[] objects = [attributesMap, dataList]
            for (routesDTO in pch.getRouteConf()?.values()) {
                //路由关系禁用
                if (routesDTO.status == 'S') continue
                //路由关系忽略
                if (routesDTO.status == 'I') continue//直接路由出去

                //开始循环分脚本
                if (pch.getSubClasses().get(routesDTO.route_name).size() > 1){
                    log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的分脚本运行方式配置异常，请检查管理表!"
                    continue
                }

                def dtoData = objects
                def size = pch.getSubClasses().get(routesDTO.route_name).values().size()
                int runI = 0;
                def flowFileDto = session.create()
                boolean bo = true
                for (subClassDTOS in pch.getSubClasses().get(routesDTO.route_name).values()) {
                    runI += 1
                    FlowFile flowFileEvent = null
                    switch (dto.status) {
                        case "A":
                            //根据路由名称 获取脚本实体GroovyObject instance
                            GroovyObject instance = pch.getScriptMapByName(dto.sub_script_name)
                            //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                            dtoData = instance.invokeMethod("calculation", dtoData)
                            //def keyData = instanceMap[key]
                            //路由对应数据
                            JSONObject data = dtoData["data"] as JSONObject
                            //路由对应属性
                            def attributes = dtoData["attributes"]
                            if ("A".equals(k)) {
                                //FlowFile put 属性
                                flowFileEvent = session.create()
                            } else {
                                flowFileEvent = flowFileDto
                            }
                            session.putAllAttributes(flowFileEvent, attributes as Map<String, String>)

                            //FlowFile write 数据
                            OutputStream outputStream
                            session.write(flowFileEvent, {
                                outputStream.write(JSONObject.toJSONBytes(data,
                                        SerializerFeature.WriteMapNullValue))
                            } as OutputStreamCallback)
                            outputStream.close()
                            bo = false
                            break;
                        case "S":
                            if ("A".equals(k)) {
                                flowFileEvent = session.clone(flowFile)
                            } else {
                                if (bo) {
                                    flowFileEvent = session.clone(flowFile)
                                    bo = false
                                } else {
                                    flowFileEvent = session.clone(flowFileDto)
                                }
                            }
                            break;
                        default:
                            break
                    }
                    //路由
                    if (null != flowFileEvent) {
                        if ("A".equals(k) || runI == size) {
                            session.transfer(flowFileEvent, pch.getRelationships().get(key))
                        } else {
                            flowFileDto = flowFileEvent
                        }
                    }
                }
                //运行方式-并行
                if (routesDTO.route_running_way == 'A') {
                    //
                } else {//串行
                    //
                }

            }
            for (String key : pch.getSubClasses().keySet()) {
                for (String k : pch.getSubClasses().get(key).keySet()) {

                    for (NifiProcessorSubClassDTO dto : pch.getSubClasses().get(key).get(k)) {

                    }
                    session.remove(flowFileDto)
                }
            }
            session.remove(flowFile)
        } catch (
                final Throwable t
                ) {
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


