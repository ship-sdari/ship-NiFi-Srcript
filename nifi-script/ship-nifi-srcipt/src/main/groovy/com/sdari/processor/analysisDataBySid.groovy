package com.sdari.processor

import com.alibaba.fastjson.JSONObject
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
    private GroovyClassLoader loader = new GroovyClassLoader()
    private Class aClass
    private GroovyObject instance
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
        aClassINit()
    }
    /**
     * 详细处理模块
     * @param context
     * @param sessionFactory
     * @throws org.apache.nifi.processor.exception.ProcessException
     */
    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession()
        final AtomicReference<JSONObject> dataMap = new AtomicReference<>()
        FlowFile flowFile = session.get()
        if (!flowFile) return
        /*以下为正常处理数据文件的部分*/
        session.read(flowFile, { inputStream ->
            try {
                dataMap.set(JSONObject.parseObject(IOUtils.toString(inputStream, StandardCharsets.UTF_8)))
            } catch (Exception e) {
                onFailure(session, flowFile)
                log.error("Failed to read from flowFile", e)
                return
            }
        })
        try {
            def attributesMap = flowFile.getAttributes()
            //[attributesMap-> flowFile属性][dataMap -> flowFile数据]
            Object[] objects = [attributesMap, dataMap]
            //执行详细脚本方法 [objects -> 详细参数][calculation ->脚本方法名]
            def instanceMap = instance.invokeMethod("calculation", objects)
            //根据路由关系 获取对应数据 [{attributes-> 路由对应属性}{data->路由对应数据}]
            for (String key : pch.getRelationships().keySet()) {
                OutputStream outputStream
                def keyData = instanceMap[key]
                //路由对应数据
                JSONObject data = keyData["data"] as JSONObject
                //路由对应属性
                def attributes = keyData["attributes"]
                FlowFile flowFileEvent = session.create()
                //FlowFile put 属性
                session.putAllAttributes(flowFileEvent, attributes as Map<String, String>)
                //FlowFile write 数据
                session.write(flowFileEvent, {
                    outputStream.write(JSONObject.toJSONBytes(data,
                            SerializerFeature.WriteMapNullValue))
                } as OutputStreamCallback)
                //路由
                session.transfer(flowFileEvent, pch.getRelationships().get(key))
                outputStream.close()
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

    /**
     * 初始化详细处理脚本
     */
    private void aClassINit() {
        def subClasses = pch.getSubClasses()
        //遍历生成分脚本（缺）
        String path = ""
        aClass = loader.parseClass(new File(path))
        instance = (GroovyObject) aClass.newInstance()
        //执行详细脚本方法 [objects -> 详细参数][calculation ->脚本方法名]
        Object[] objects = []
        instance.invokeMethod("calculation", objects)
    }
}


