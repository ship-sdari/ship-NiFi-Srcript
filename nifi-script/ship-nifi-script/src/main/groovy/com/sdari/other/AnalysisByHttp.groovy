package com.sdari.other

import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.annotation.lifecycle.OnStopped
import org.apache.nifi.components.ValidationContext
import org.apache.nifi.components.ValidationResult
import org.apache.nifi.dbcp.DBCPService
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSessionFactory
import org.apache.nifi.processor.Processor
import org.apache.nifi.processor.ProcessorInitializationContext


import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.components.PropertyDescriptor

import org.apache.nifi.processor.Relationship
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.processor.util.StandardValidators

import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.text.MessageFormat
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription("Execute a series of JDBC queries adding the results to each JSON presented in the FlowFile")
class AnalysisByHttp implements Processor {

    def log

    final static Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description('FlowFiles that were successfully processed and had any data enriched are routed here')
            .build()

    final static Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description('FlowFiles that were not successfully processed are routed here')
            .build()
    private final PropertyDescriptor CALLBACK_TYPE = new PropertyDescriptor.Builder()
            .name("Rules callback type")
            .required(true)
            .description("规则类型")
            .defaultValue("HTTP")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final PropertyDescriptor SQL = new PropertyDescriptor.Builder()
            .name("sql O(语句一)")
            .required(true)
            .description("TCP distributes IP and ports separated by semicolons(指标查询sql)")
            .defaultValue("SELECT colgroup FROM `tstream_rule` where status = 'A' and protocal = ''{0}'' GROUP  BY colgroup;")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    final static PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("dbcp-connection-pool-services")
            .displayName("Database Connection Pool Services")
            .description("The Controller Service that is used to obtain a connection to the database.")
            .required(true)
            .identifiesControllerService(DBCPService)
            .build()

    Set<Relationship> getRelationships() { [REL_FAILURE, REL_SUCCESS] as Set }

    private String callBackType = null
    protected DBCPService dbcpService
    private String cog

    @Override
    List<PropertyDescriptor> getPropertyDescriptors() {
        Collections.unmodifiableList([DBCP_SERVICE,callBackType,SQL]) as List<PropertyDescriptor>
    }
    /**
     * A method that executes only once when initialized
     *
     * @param context
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            callBackType = context.getProperty(CALLBACK_TYPE).getValue()
            dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class)
            updateTStreamRules(context)
        }
        catch (Exception e) {
            log.error " 处理器起始运行异常", e

        }
    }
    @OnStopped
    public void OnStopped(final ProcessContext context) {
            log.error " 处理器关闭异常"
    }
    void initialize(ProcessorInitializationContext context) {}


    void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        def session = sessionFactory.createSession()
        def flowFile = session.get()
        if (!flowFile) return
        /*以下为正常处理数据文件的部分*/
        final AtomicReference<String> datas = new AtomicReference<>()
        session.read(flowFile, { inputStream ->
            try {
                datas.set(IOUtils.toString(inputStream, StandardCharsets.UTF_8))
            } catch (Exception ignored) {
                session.transfer(flowFile, REL_FAILURE)
                session.commit()
            }
        })
        try {
            if (null == datas.get()) {
                throw new Exception(" 接收的数据为空!")
            }
            Map attr = new HashMap()
            attr.put("colgroup", cog)
            attr.put("coltime", String.valueOf(Instant.now()))
            String maps = datas.get()
            String data = maps.substring(maps.indexOf("[") + 1, maps.indexOf("]"))
            FlowFile flowFileNew = session.create()
            //FlowFile write 数据
            session.putAllAttributes(flowFileNew, attr)
            session.write(flowFileNew, { out ->
                out.write(data.getBytes(StandardCharsets.UTF_8))
            } as OutputStreamCallback)
            session.transfer(flowFileNew, REL_SUCCESS)
            session.remove(flowFile)
        } catch (final Throwable t) {
            log.error(' failed to process due to ' + t)
            session.transfer(flowFile, REL_FAILURE)
        } finally {
            session.commit()
        }
    }


    @Override
    Collection<ValidationResult> validate(ValidationContext context) { null }

    @Override
    PropertyDescriptor getPropertyDescriptor(String name) {
        PropertyDescriptor.NULL_DESCRIPTOR
    }

    @Override
    void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {}

    @Override
    String getIdentifier() { null }
    /**
     * 设置该处理器的logger
     * @param logger
     * @throws Exception
     */
    void setLogger(final ComponentLog logger) {
        try {
            log = logger
        } catch (Exception e) {
            log.error "设置日志的调用方法异常", e
        }
    }
    /**
     * 查询流配置表并按照采集组更新
     *
     * @param context
     * @throws Exception
     */
    void updateTStreamRules(final ProcessContext context) throws Exception {
        try {
            //mysql connection
            final String sql = context.getProperty(SQL).getValue()
            final String type = context.getProperty(CALLBACK_TYPE).getValue()
            final Connection conn = dbcpService.getConnection()
            Statement stmt1 = conn.createStatement()
            String qSql = MessageFormat.format(sql, type)
            ResultSet result = stmt1.executeQuery(qSql)
            while (result.next()) {
                cog = result.getObject(1)
            }
            result.close()
            stmt1.close()
            conn.close()
        } catch (Exception e) {
            throw new Exception(e)
        }
    }
}

//processor = new AnalysisByHttp()
