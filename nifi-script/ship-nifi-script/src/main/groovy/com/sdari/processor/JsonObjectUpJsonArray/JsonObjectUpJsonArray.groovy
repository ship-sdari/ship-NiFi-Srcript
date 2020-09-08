package com.sdari.processor.JsonObjectUpJsonArray


import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.ProcessSessionFactory
import org.apache.nifi.processor.Processor
import org.apache.nifi.processor.ProcessorInitializationContext


import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.OutputStreamCallback

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription("Execute a series of JDBC queries adding the results to each JSON presented in the FlowFile")
class JsonObjectUpJsonArray implements Processor {

    def log
    private String id
    final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description('FlowFiles that were successfully processed and had any data enriched are routed here')
            .build()
    final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description('FlowFiles that were not successfully processed are routed here')
            .build()

    Set<Relationship> getRelationships() { [REL_FAILURE, REL_SUCCESS] as Set }

    void scriptByInitId(pid, service) throws Exception {}

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        Collections.unmodifiableList([]) as List<PropertyDescriptor>
    }

    void initialize(ProcessorInitializationContext context) {}

    /**
     * A method that executes only once when initialized
     *
     * @param context
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            log.info " 处理器起始运行完毕"
        } catch (Exception e) {
            log.error " 处理器起始运行异常", e
        }
    }

    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession()
        FlowFile flowFile = session.get()
        if (flowFile == null) {
            session.commit()
            return
        }
        //以下为正常处理数据文件的部分
        final AtomicReference<JSONObject> dataList = new AtomicReference<>()
        session.read(flowFile, { inputStream ->
            try {
                dataList.set(JSONObject.parseObject(IOUtils.toString(inputStream, StandardCharsets.UTF_8)))
            } catch (Exception e) {
                log.error " 读取流文件失败", e
                onFailure(session, flowFile)
                session.commit()
                return
            }
        })
        try {
            if (null == dataList.get() || dataList.get().size() == 0) {
                throw new Exception("接收的数据为空!")
            }
            JSONObject JsonData = dataList.get()
            def jsons = []
            jsons.add(JsonData)
            session.putAllAttributes(flowFile, flowFile.getAttributes())
            session.write(flowFile, { out ->
                out.write(JSONArray.toJSONBytes(jsons, SerializerFeature.WriteMapNullValue))
            } as OutputStreamCallback)
            session.transfer(flowFile, REL_SUCCESS)
        } catch (final Throwable t) {
            log.error "[Processor_id = ${id} 数据处理异常", t
            session.transfer(flowFile, REL_FAILURE)
        } finally {
            session.commit()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        null
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
    private static void onFailure(final ProcessSession session, final FlowFile flowFile) {
        session.transfer(flowFile, REL_FAILURE)
    }

    /**
     * 设置该处理器的logger
     * @param logger
     * @throws Exception
     */
    void setLogger(final ComponentLog logger) {
        log = logger
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
    }
}

//processor = new JsonObjectUpJsonArray()



