package com.sdari.publicUtils

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
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

/**
 * GroovyMainUtils
 */
@EventDriven
@CapabilityDescription()
class GroovyMainUtils implements Processor {

    static def log
    public static String id
    private DBCPService dbcpService = null
    private Map<String, PropertyDescriptor> descriptorMap = new HashMap<>()
    private Map<String, Relationship> relationshipMap = new HashMap<>()
    private GroovyClassLoader loader = new GroovyClassLoader()
    private Class aClass
    private GroovyObject instance
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Where all bad people end up(异常)")
            .build()

    Set<Relationship> getRelationships() {
        Set<Relationship> set = new HashSet<Relationship>()
        set.add(REL_FAILURE)
        for (String relation : relationshipMap.keySet()) {
            Relationship relationship = relationshipMap.get(relation)
            set.add(relationship)
        }
        return Collections.unmodifiableSet(set) as Set<Relationship>
    }

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        List<PropertyDescriptor> descriptorList = new ArrayList<>()
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
            for (String key : relationshipMap.keySet()) {
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
                session.transfer(flowFileEvent, relationshipMap.get(key))
                outputStream.close()
            }
            session.remove(flowFile)
        } catch (final Throwable t) {
            log.error('{} failed to process due to {}', [this, t] as Object[])
            session.transfer(flowFile, REL_FAILURE)
        } finally {
            session.commit()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        return descriptorMap.get(name)
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
    private static void onFailure(final ProcessSession session, final FlowFile flowFile) {
        session.transfer(flowFile, REL_FAILURE)
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
        relationshipInit()
    }

    /**
     * 根据 脚本id 加载路由关系
     * @param id 脚本id
     */
    private void relationshipInit(String id) {
        log.info("脚本id: ", id)
        final Relationship REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("FlowFiles that were successfully processed")
                .build()
        final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .description("FlowFiles that were successfully processed")
                .build()
        relationshipMap.put("failure", REL_FAILURE)
        relationshipMap.put("success", REL_SUCCESS)
    }

    /**
     * 初始化详细处理脚本
     */
    private void aClassINit() {
        String path = ""
        aClass = loader.parseClass(new File(path))
        instance = (GroovyObject) aClass.newInstance()
        //执行详细脚本方法 [objects -> 详细参数][calculation ->脚本方法名]
        Object[] objects = []
        instance.invokeMethod("calculation", objects)
    }
}
