import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.ArrayUtils
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
class testB implements Processor {

    def log
    //新增
    final String ADD = '0'
    //先删除后新增
    final String DELETE_ADD = '1'
    //更新
    final String UPDATE = '2'
    //删除
    final String DELETE = '3'

    //数据处理使用参数
    final String SID = 'sid'
    final String STATUS = 'status'
    final String DATA = 'data'
    final String TABLE_NAME = 'tableName'
    final String OPTION = 'option'
    final String META = 'meta'
    //
    final String relationName = 'relationName'
    final String[] FileTables = { ['t_calculation', 't_alarm_history'] }
    final String optionSTATUS = "optionSTATUS"
    final static Relationship REL_MySql = new Relationship.Builder()
            .name("mysql")
            .description('FlowFiles that were successfully processed and had any data enriched are routed here')
            .build()
    final static Relationship REL_ES = new Relationship.Builder()
            .name("es")
            .description('FlowFiles that were successfully processed and had any data enriched are routed here')
            .build()
    final static Relationship REL_Hive = new Relationship.Builder()
            .name("hive")
            .description('FlowFiles that were successfully processed and had any data enriched are routed here')
            .build()

    final static Relationship REL_HBase = new Relationship.Builder()
            .name("HBase")
            .description('FlowFiles that were successfully processed and had any data enriched are routed here')
            .build()
    final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description('FlowFiles that were not successfully processed are routed here')
            .build()

    Set<Relationship> getRelationships() { [REL_FAILURE, REL_MySql, REL_ES, REL_Hive, REL_HBase] as Set }


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
        if (flowFile == null) session.commit()
        if (!flowFile) return
        /*以下为正常处理数据文件的部分*/
        final AtomicReference<JSONObject> dataList = new AtomicReference<>()
        session.read(flowFile, { inputStream ->
            try {
                dataList.set(JSONObject.parseObject(IOUtils.toString(inputStream, StandardCharsets.UTF_8)))
            } catch (Exception e) {
                log.error " 读取流文件失败", e
                onFailure(session, flowFile)
                session.commit()
            }
        })
        try {
            if (null == dataList.get() || dataList.get().size() == 0) {
                throw new Exception("接收的数据为空!")
            }
            JSONObject JsonData = dataList.get()

            def metaMpa = JsonData.get(META) as JSONObject
            final String sid = metaMpa.get(SID)
            final String tableName = metaMpa.get(TABLE_NAME)
            final String status = metaMpa.get(STATUS)
            final String option = metaMpa.get(OPTION)
            JSONArray data = metaMpa.get(DATA) as JSONArray
            Map<String, String> map = new HashMap<>()
            map.put(SID, sid)
            map.put(STATUS, status)
            map.put(OPTION, option)
            map.put(TABLE_NAME, tableName)

            for (json in data) {
                json as JSONObject
                FlowFile flowFile1 = session.create()
                session.putAllAttributes(flowFile1, map)
                session.write(flowFile1, { out ->
                    out.write(JSONObject.toJSONBytes(data,
                            SerializerFeature.WriteMapNullValue))
                } as OutputStreamCallback)
                if (!ArrayUtils.contains(FileTables, tableName.toLowerCase())) {
                    FlowFile flowFile2 = session.clone(flowFile1)
                    switch (option) {
                        case ADD:
                            session.putAttribute(flowFile2, optionSTATUS, ADD)
                            break
                        case DELETE_ADD:
                            session.putAttribute(flowFile2, optionSTATUS, DELETE_ADD)
                            break
                        case UPDATE:
                            session.putAttribute(flowFile2, optionSTATUS, UPDATE)
                            break
                        case DELETE:
                            session.putAttribute(flowFile2, optionSTATUS, DELETE)
                            break
                        default:
                            session.putAttribute(flowFile2, optionSTATUS, "null")
                    }
                    session.putAttribute(flowFile2, relationName, "MySql")
                    session.transfer(flowFile, REL_MySql)
                } else {
                    FlowFile flowFile2 = session.clone(flowFile1)
                    session.putAttribute(flowFile2, relationName, "ES")
                    session.transfer(flowFile1, REL_ES)
                }
                session.putAttribute(flowFile1, relationName, "HBase")
                session.transfer(flowFile1, REL_HBase)
            }
            session.transfer(flowFile, REL_Hive)
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
}

processor = new testB()



