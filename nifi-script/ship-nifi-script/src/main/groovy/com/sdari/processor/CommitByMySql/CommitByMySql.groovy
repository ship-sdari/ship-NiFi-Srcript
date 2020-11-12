package com.sdari.processor.CommitByMySql

import com.alibaba.fastjson.JSONArray
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

import java.nio.charset.StandardCharsets
import java.sql.*
import java.text.MessageFormat
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@EventDriven
@CapabilityDescription('岸基-MySql业务提交处理器处理器')
class CommitByMySql implements Processor {
    static def log
    //处理器id，同处理器管理表中的主键一致，由调度处理器中的配置同步而来
    private String id
    private String currentClassName = this.class.canonicalName
    private DBCPService dbcpService = null
    private GroovyObject pch

    //数据处理使用参数
    final static String databaseName = 'database.name'
    //处理器使用相关参数
    private String ip
    private String port
    private String userName
    private String password
    private String driverByClass
    //库名->url
    private Map<String, String> databases = new HashMap<>()
    //库名->数据库连接
    private Map<String, Connection> connections = new HashMap<>()
    private static final String url = "jdbc:mysql://{0}:{1}/{2}?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useLegacyDatetimeCode=false&useSSL=false&testOnBorrow=true"

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
            pch.invokeMethod("initScript", [log, currentClassName, pch])
            Map confMap = pch.getProperty('parameters') as Map
            initConf(confMap)//初始化连接配置
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行完毕"
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器起始运行异常", e
        }
    }

    @OnStopped
    public void OnStopped(final ProcessContext context) {
        try {
            pch.invokeMethod("releaseComponent", null)//相关公共配置实例清空
            log.info "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器停止运行完毕"
            for (String name : connections.keySet()) {
                if (null != connections.get(name) && connections.get(name).isClosed()) {
                    connections.get(name).isClosed().clone()
                }
            }
        } catch (Exception e) {
            log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 处理器关闭异常", e
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
            final Map<String, String> attributesMap = pch.invokeMethod("updateAttributes", [flowFile.getAttributes()]) as Map<String, String>
            //调用脚本需要传的参数[attributesMap-> flowFile属性][data -> flowFile数据]
            def dataList = []
            switch (datas.get().getClass().canonicalName) {
                case 'com.alibaba.fastjson.JSONObject':
                    dataList.add(datas.get())
                    break
                case 'com.alibaba.fastjson.JSONArray':
                    dataList = datas.get()
                    break
                default:
                    throw new Exception("暂不支持处理当前所接收的数据类型：${datas.get().getClass().canonicalName}")
            }
            //循环路由名称 根据路由状态处理 [路由名称->路由实体]
            final String databaseName = attributesMap.get(databaseName)
            List<String> data = dataList as List<String>
            def logs = log
            boolean status = transaction(data, databaseName, logs)
            try {
                if (status) onFailure(session,flowFile)
                if (!status && relationships.containsKey('success')) {
                    session.transfer(flowFile, relationships.get('success'))
                }
            } catch (Exception e) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 数据路由异常", e
            }
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

    /**
     * 初始化所有连接配置
     */
    void initConf(Map<String, String> confMap) throws Exception {
        ip = confMap.get("ip")
        port = confMap.get("port")
        userName = confMap.get("user.name")
        password = confMap.get("password")
        driverByClass = confMap.get("driver.class")
    }
    /**
     * 根据数据库名 创建连接
     * @param database 数据库名
     */
    void ConnectionsInIt(String database) throws Exception {
        String u = url
        String url = MessageFormat.format(u, ip, port, database)
        Class.forName(driverByClass).newInstance()
        connections.put(database, DriverManager.getConnection(url, userName, password))
    }

    /**
     * 报警事件事务提交方法（同步锁）
     *
     * @param contents
     * @param session
     * @throws Exception
     */
    private synchronized boolean transaction(List<String> contents, String database, def log) throws Exception {
        boolean isError = false
        //如果没有库的连,或者连接断开 就新建一个连接
        if (!connections.containsKey(database) || null == connections.get(database)
                || connections.get(database).isClosed()) {
            ConnectionsInIt(database)
        }
        Connection connection = connections.get(database)
        if (connection.getAutoCommit()) connection.setAutoCommit(false);//关闭自动提交
        Savepoint savepoint = connection.setSavepoint("current")
        PreparedStatement stmt1
        for (String content : contents) {
            try {
                stmt1 = connection.prepareStatement(content)
                stmt1.executeUpdate()
                if (!stmt1.isClosed()) stmt1.close()
            } catch (Exception e) {
                log.error "[Processor_id = ${id} Processor_name = ${currentClassName}] 的处理过程有异常,报警事务操作执行异常", e
                isError = true
            }
        }
        if (isError) {
            connection.rollback(savepoint)//回滚
            connection.releaseSavepoint(savepoint)
        } else {
            connection.commit()//提交
        }
        return isError
    }
}

//脚本部署时需要放开该注释
//processor = new CommitByMySql()
