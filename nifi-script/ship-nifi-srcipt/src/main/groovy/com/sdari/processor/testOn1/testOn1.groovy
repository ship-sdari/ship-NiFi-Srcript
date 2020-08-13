package com.sdari.processor.testOn1
/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/13 14:44
 */
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.io.IOUtils
import org.apache.nifi.annotation.behavior.EventDriven
import org.apache.nifi.annotation.documentation.CapabilityDescription
import org.apache.nifi.annotation.lifecycle.OnScheduled
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
    private static GroovyClassLoader loader = new GroovyClassLoader()

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
            final Object[] args = [[pch.returnRules: pch.getTStreamRules(), pch.returnAttributes: attributesMap, pch.returnData: dataList.get()]]

            //循环路由名称 根据路由状态处理 [路由名称->路由实体]
            for (routesDTO in pch.getRouteConf()?.values()) {
                if ('A' == routesDTO.route_running_way) {
                    log.error "处理器：" + id + "路由：" + routesDTO.route_name + "的运行方式，暂不支持并行执行方式，请检查管理表!"
                    continue
                }
                //用来接收脚本返回的数据
                Object[] returnList = args
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
                            //执行方式 A-并行 S-串行
                            if ("S" == runningWay) {
                                for (subClassDTO in pch.getSubClasses().get(routesDTO.route_name).get(runningWay)) {
                                    if ('A' == subClassDTO.status) {
                                        //根据路由名称 获取脚本实体GroovyObject instance
                                        final GroovyObject instance = pch.getScriptMapByName(subClassDTO.sub_script_name)
                                        //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                                        returnList = instance.invokeMethod(pch.funName, returnList)
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
                        for (data in returnList[0][pch.returnData]) {
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
    public void scriptByInitId(pid, service) throws Exception {
        id = pid
        dbcpService = service
        pch = new ProcessorComponentHelper(id as int, dbcpService.getConnection())
    }
}

@Data
class TStreamRuleDTO {
    //  配置规则号
    private Integer rule_id
    // 船id
    private Integer sid
    //船舶ID
    private String ship_id

    //系统ID
    private Integer sys_id

    // DOSS系统key值
    private Integer doss_key

    //通讯协议
    private String protocol

    // 信号中文名
    private String name_chn

    //信号英文名
    private String name_eng

    // 原始key值
    private String orig_key

    //     数据来源标志位
    private String data_from

    //单位
    private String unit

    //量纲转换因子
    private BigDecimal transfer_factor

    //系数
    private Double coefficient

    //启用状态
    // A - 活跃
    // S - 暂时禁用
    // D - 删除"
    private String status

    //开关量/模拟量
    private String value_type

    //量程最小值
    private BigDecimal value_min

    //量程最小值
    private BigDecimal value_max

    // 应用名称
    private String inner_key

    //输入时间
    private String input_time

    //输入用户
    private String input_user

    //最后更新时间
    private String last_modify_time

    //最后更新用户
    private String last_modify_user

    //报警分表
    private List<AlarmDTO> alarm = []

    //计算分表
    private List<CalculationDTO> calculation = []

    //采集分表
    private List<CollectionDTO> collection = []

    //第三方分发分表
    private List<DistDTO> other_distributions = []

    //岸基分发分表
    private List<ShoreBasedDTO> shore_based_distributions = []

    //抽稀分表
    private List<ThinningDTO> thinning = []

    //入库分表
    private List<WarehousingDTO> warehousing = []

    @Data
    static class AlarmDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        // 报警最大值，最小值范围
        private BigDecimal alert_min
        private BigDecimal alert_max
        private BigDecimal alert_2nd_min
        private BigDecimal alert_2nd_max
        //关联设备状态标志位
        private String relate_stop_sig
        //    报警启用状态字段
        private String alert_status
        //    报警是否弹窗
        private String is_popup
        //    AMS报警报警参考
        private Integer ams_alarm_standard
        //    报警方式
        private String alert_way
    }

    @Data
    static class CalculationDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //  参与指标计算所转换的key值
        private String calculation_key
        //指标名称标志位
        private String formula_flag
    }

    @Data
    static class CollectionDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        // 采集组编号
        private String colgroup
        //SlaveID从站编号
        private Integer modbus_slave_id
        //modbus操作功能
        private Integer modbus_func_id
        //modbus寄存器地址
        private String addr
        //数据来源IP地址
        private String ip_addr
        //    用于链路中断暂存字段
        private String ip_addr_down
        //端口号
        private String port_addr
        //    来源表名
        private String from_table_id
        //   来源列名
        private String from_column_id
        //通讯协议
        private String protocol
        //采样频率
        private String col_freq
        // 主题
        private String topic
        // modbus信号标签
        private String modbus_sig_tag
        //数据包请求间隔
        private String col_interval
        //数据包请求数量
        private int col_count
        //nema0183_config关联id
        private Long nmea_id
    }

    @Data
    static class DistDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //数据分发分组
        private String dist_group
        //数据分发目的IP
        private String dist_ip_addr

        //    链路中断暂存字段
        private String dist_ip_addr_down

        //  数据分发频率
        private String dist_freq

        //数据分发协议
        private String dist_protocol

        //  用于SFTP分发的用户名和密码
        private String dis_user_and_password_down

        //    链路中断暂存字段
        private String dis_user_and_password
    }

    @Data
    static class ShoreBasedDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //船岸传输组
        private String to_shore_group
        //船岸传输目的IP
        private String to_shore_ip_addr
        //船岸传输频率
        private String to_shore_freq
        //船岸传输协议
        private String to_shore_protocol
        //岸基压缩方式
        private String compress_type
    }

    @Data
    static class ThinningDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //    抽稀频率
        private String sparse_rate
        //1.求累计 2.求平均 3.只取点
        private int dilution_type
    }

    @Data
    static class WarehousingDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //    入库库名
        private String schema
        //数据表名
        private String table_id
        //列名
        private String column_id
        //数据类型
        private String data_type
    }

    static Map<String, Map<String, TStreamRuleDTO>> createDto(ResultSet resBasic, ResultSet resAlarm, ResultSet resCalculation, ResultSet resCollection, ResultSet resDist, ResultSet resShoreBased, ResultSet resThinning, ResultSet resWarehousing) throws Exception {
        try {
            def createBasicDto = { dto, res ->
                dto.rule_id = res.getInt('rule_id')
                dto.sid = res.getInt('sid')
                dto.ship_id = res.getString('ship_id')
                dto.sys_id = res.getInt('sys_id')
                dto.doss_key = res.getInt('doss_key')
                dto.protocol = res.getString('protocol')
                dto.name_chn = res.getString('name_chn')
                dto.name_eng = res.getString('name_eng')
                dto.orig_key = res.getString('orig_key')
                dto.data_from = res.getString('data_from')
                dto.unit = res.getString('unit')
                dto.transfer_factor = res.getBigDecimal('transfer_factor')
                dto.coefficient = res.getBigDecimal('coefficient')
                dto.status = res.getString('status')
                dto.value_type = res.getString('value_type')
                dto.value_min = res.getBigDecimal('value_min')
                dto.value_max = res.getBigDecimal('value_max')
                dto.inner_key = res.getString('inner_key')
            }
            def createAlarmDto = { dto, res ->
                dto.sid = res.getInt('sid')
                dto.doss_key = res.getInt('doss_key')
                dto.alert_min = res.getBigDecimal('alert_min')
                dto.alert_max = res.getBigDecimal('alert_max')
                dto.alert_2nd_min = res.getBigDecimal('alert_2nd_min')
                dto.alert_2nd_max = res.getBigDecimal('alert_2nd_max')
                dto.relate_stop_sig = res.getString('relate_stop_sig')
                dto.alert_status = res.getString('alert_status')
                dto.is_popup = res.getString('is_popup')
                dto.ams_alarm_standard = res.getInt('ams_alarm_standard')
                dto.alert_way = res.getString('alert_way')
            }
            def createCalculationDto = { dto, res ->
                dto.sid = res.getInt('sid')
                dto.doss_key = res.getInt('doss_key')
                dto.calculation_key = res.getString('calculation_key')
                dto.formula_flag = res.getString('formula_flag')
            }
            def createCollectionDto = { dto, res ->
                dto.sid = res.getInt('sid')
                dto.doss_key = res.getInt('doss_key')
                dto.colgroup = res.getString('colgroup')
                dto.modbus_slave_id = res.getInt('modbus_slave_id')
                dto.modbus_func_id = res.getInt('modbus_func_id')
                dto.addr = res.getString('addr')
                dto.ip_addr = res.getString('ip_addr')
                dto.ip_addr_down = res.getString('ip_addr_down')
                dto.port_addr = res.getString('port_addr')
                dto.from_table_id = res.getString('from_table_id')
                dto.from_column_id = res.getString('from_column_id')
                dto.protocol = res.getString('protocol')
                dto.col_freq = res.getString('col_freq')
                dto.topic = res.getString('topic')
                dto.modbus_sig_tag = res.getString('modbus_sig_tag')
                dto.col_interval = res.getString('col_interval')
                dto.col_count = res.getInt('col_count')
                dto.nmea_id = res.getLong('nmea_id')
            }
            def createDistDto = { dto, res ->
                dto.sid = res.getInt('sid')
                dto.doss_key = res.getInt('doss_key')
                dto.dist_group = res.getString('dist_group')
                dto.dist_ip_addr = res.getString('dist_ip_addr')
                dto.dist_ip_addr_down = res.getString('dist_ip_addr_down')
                dto.dist_freq = res.getString('dist_freq')
                dto.dist_protocol = res.getString('dist_protocol')
                dto.dis_user_and_password_down = res.getString('dis_user_and_password_down')
                dto.dis_user_and_password = res.getString('dis_user_and_password')
            }
            def createShoreBasedDto = { dto, res ->
                dto.sid = res.getInt('sid')
                dto.doss_key = res.getInt('doss_key')
                dto.to_shore_group = res.getString('to_shore_group')
                dto.to_shore_ip_addr = res.getString('to_shore_ip_addr')
                dto.to_shore_freq = res.getString('to_shore_freq')
                dto.to_shore_protocol = res.getString('to_shore_protocol')
                dto.compress_type = res.getString('compress_type')
            }
            def createThinningDto = { dto, res ->
                dto.sid = res.getInt('sid')
                dto.doss_key = res.getInt('doss_key')
                dto.sparse_rate = res.getString('sparse_rate')
                dto.dilution_type = res.getInt('dilution_type')
            }
            def createWarehousingDto = { dto, res ->
                dto.sid = res.getInt('sid')
                dto.doss_key = res.getInt('doss_key')
                dto.schema = res.getString('schema')
                dto.table_id = res.getString('table_id')
                dto.column_id = res.getString('column_id')
                dto.data_type = res.getString('data_type')
            }
            Map<String, Map<String, TStreamRuleDTO>> TStreamRules = [:]
            //遍历基础表
            while (resBasic.next()) {
                TStreamRuleDTO basicDto = new TStreamRuleDTO()
                //基础表闭包调用
                createBasicDto.call(basicDto, resBasic)
                if (!TStreamRules.containsKey(basicDto.sid as String)) {
                    TStreamRules.put(basicDto.sid as String, [:])
                }
                TStreamRules.get(basicDto.sid as String)[basicDto.doss_key as String] = basicDto
            }
            //遍历报警表
            while (resAlarm.next()) {
                AlarmDTO alarmDto = new AlarmDTO()
                createAlarmDto.call(alarmDto, resAlarm)
                TStreamRules.get(alarmDto.sid as String)?.get(alarmDto.doss_key as String)?.alarm?.add(alarmDto)
            }
            //遍历计算表
            while (resCalculation.next()) {
                CalculationDTO calculationDto = new CalculationDTO()
                createCalculationDto.call(calculationDto, resCalculation)
                TStreamRules.get(calculationDto.sid as String)?.get(calculationDto.doss_key as String)?.calculation?.add(calculationDto)
            }
            //遍历采集表
            while (resCollection.next()) {
                CollectionDTO collectionDto = new CollectionDTO()
                createCollectionDto.call(collectionDto, resCollection)
                TStreamRules.get(collectionDto.sid as String)?.get(collectionDto.doss_key as String)?.collection?.add(collectionDto)
            }
            //遍历第三方分发表
            while (resDist.next()) {
                DistDTO distDto = new DistDTO()
                createDistDto.call(distDto, resDist)
                TStreamRules.get(distDto.sid as String)?.get(distDto.doss_key as String)?.other_distributions?.add(distDto)
            }
            //遍历岸基分发表
            while (resShoreBased.next()) {
                ShoreBasedDTO shoreBasedDto = new ShoreBasedDTO()
                createShoreBasedDto.call(shoreBasedDto, resShoreBased)
                TStreamRules.get(shoreBasedDto.sid as String)?.get(shoreBasedDto.doss_key as String)?.shore_based_distributions?.add(shoreBasedDto)
            }
            //遍历抽稀表
            while (resThinning.next()) {
                ThinningDTO thinningDto = new ThinningDTO()
                createThinningDto.call(thinningDto, resThinning)
                TStreamRules.get(thinningDto.sid as String)?.get(thinningDto.doss_key as String)?.thinning?.add(thinningDto)
            }
            //遍历入库表
            while (resWarehousing.next()) {
                WarehousingDTO warehousingDto = new WarehousingDTO()
                createWarehousingDto.call(warehousingDto, resWarehousing)
                TStreamRules.get(warehousingDto.sid as String)?.get(warehousingDto.doss_key as String)?.warehousing?.add(warehousingDto)
            }
            TStreamRules
        } catch (Exception e) {
            throw new Exception("com.sdari.processor.testOn1.NifiProcessorSubClassDTO createDto has an error", e)
        }
    }
}


import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.Relationship
import java.sql.Connection
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean

/**
 * This class contains variables and methods common to processors.
 */

class ProcessorComponentHelper {

    final AtomicBoolean isInitialized = new AtomicBoolean(false)
    private int processorId
    private Map<String, PropertyDescriptor> descriptors
    private Map<String, Relationship> relationships
    private Map parameters
    private Map<String, NifiProcessorRoutesDTO> routeConf
    private Map<String, Map<String, List<NifiProcessorSubClassDTO>>> subClasses
    private Map<String, GroovyObject> scriptMap
    private Map<String, Map<String, TStreamRuleDTO>> tStreamRules
//    private String url = 'jdbc:mysql://10.0.16.19:3306/groovy?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useLegacyDatetimeCode=false&useSSL=false&testOnBorrow=true&validationQuery=select 1'
//    private String userName = 'appuser'
//    private String password = 'Qgy@815133'
//    private int timeOut = 10
    private Connection con
    //脚本的方法名
    public final static String funName = "calculation"
    //脚本返回的数据
    public final static String returnData = "data"
    //脚本返回的属性
    public final static String returnAttributes = "attributes"
    //脚本返回的配置
    public final static String returnRules = "rules"

    ProcessorComponentHelper(int id, Connection con) {
        //构造处理器编号
        processorId = id
        //构造管理库连接
        loadConnection(con)
        //根据管理库连接查询所有结果并暂存
        //缺
    }

    void loadConnection(Connection con) {
        if ((this.con == null || this.con.isClosed()) && (con != null && !con.isClosed())) {
//            Class.forName('com.mysql.jdbc.Driver').newInstance()
//            DriverManager.setLoginTimeout(timeOut)
//            con = DriverManager.getConnection(url, userName, password)
            this.con = con
            con.setReadOnly(true)
        }
    }

    void releaseConnection() {
        if (con != null && !con.isClosed()) {
            con.close()
        }
    }

    Map<String, NifiProcessorRoutesDTO> getRouteConf() {
        return this.routeConf
    }

    void setRouteConf(Map<String, NifiProcessorRoutesDTO> routeConf) {
        this.routeConf = routeConf
    }

    Map<String, PropertyDescriptor> getDescriptors() {
        return descriptors
    }

    void setDescriptors(Map<String, PropertyDescriptor> descriptors) {
        this.descriptors = descriptors
    }

    Map<String, Relationship> getRelationships() {
        return relationships
    }

    void setRelationships(Map<String, Relationship> relationships) {
        this.relationships = relationships
    }

    def getParameters() {
        return this.parameters
    }

    void setParameters(parameters) {
        this.parameters = parameters
    }

    def getSubClasses() {
        return this.subClasses
    }

    def getScriptMapByName(String name) {
        return this.scriptMap.get(name)
    }

    void setSubClasses(subClassGroups) {
        this.subClasses = subClassGroups
    }

    def getTStreamRules() {
        return this.tStreamRules
    }

    void setTStreamRules(tStreamRuleDto) {
        this.tStreamRules = tStreamRuleDto
    }

    void createDescriptors() {
        descriptors = [:]

        // descriptors.add(routes_manager_utils.SCRIPT_FILE)
        // descriptors.add(routes_manager_utils.SCRIPT_BODY)
        // descriptors.add(routes_manager_utils.MODULES)

        this.isInitialized.set(true)
    }

    void createRelationships(List<String> names) {
        relationships = [:]
        relationships.putAll(RoutesManagerUtils.createRelationshipMap(names))
    }

    void createParameters(List<NifiProcessorAttributesDTO> attributeRows) {
        parameters = [:]
        parameters.putAll(AttributesManagerUtils.createAttributesMap(attributeRows))
    }

    void createSubClasses(List<NifiProcessorSubClassDTO> subClasses, Map<Integer, NifiProcessorRoutesDTO> routeMap) {
        Map<String, Map<String, List<NifiProcessorSubClassDTO>>> subClassGroups = [:]
        subClasses?.each {
            final int route_id = it.getProperty('route_id') as int
            final String route_name = routeMap.get(route_id).getProperty('route_name')
            final String sub_running_way = it.getProperty('sub_running_way')
            if (!subClassGroups.containsKey(route_name)) subClassGroups.put(route_name, [:])
            if (!subClassGroups.get(route_name).containsKey(sub_running_way)) subClassGroups.get(route_name).put(sub_running_way, [])
            subClassGroups.get(route_name).get(sub_running_way).add(it)
        }
        setSubClasses(subClassGroups)
    }

    void createTStreamRules(Map<String, Map<String, TStreamRuleDTO>> tStreamRuleDto) {
        setTStreamRules(tStreamRuleDto)
    }

    void initComponent() throws Exception {
        loadConnection()
        //闭包查询路由表
        List<NifiProcessorRoutesDTO> routesDto = null
        def selectRouteManagers = {
            def routesSelect = "SELECT * FROM `nifi_processor_route` WHERE `processor_id` = ${processorId};"
            Statement stm = con.createStatement()
            ResultSet res = stm.executeQuery(routesSelect)
            routesDto = NifiProcessorRoutesDTO.createDto(res)
            if (!res.closed) res.close()
            if (!stm.isClosed()) stm.close()
        }
        selectRouteManagers.call()
        //闭包查询属性表
        List<NifiProcessorAttributesDTO> attributesDto = null
        def selectAttributeManagers = {
            def attributesSelect = "SELECT * FROM `nifi_processor_attributes` WHERE `processor_id` = ${processorId};"
            Statement stm = con.createStatement()
            ResultSet res = stm.executeQuery(attributesSelect)
            attributesDto = NifiProcessorAttributesDTO.createDto(res)
            if (!res.closed) res.close()
            if (!stm.isClosed()) stm.close()
        }
        selectAttributeManagers.call()
        //闭包查询子脚本表
        def subClassesDto = null
        def selectSubClassManagers = {
            def subClassesSelect = "SELECT * FROM `nifi_processor_sub_class` WHERE `processor_id` = ${processorId} ORDER BY `running_order`;"
            Statement stm = con.createStatement()
            ResultSet res = stm.executeQuery(subClassesSelect)
            subClassesDto = NifiProcessorSubClassDTO.createDto(res)
            if (!res.closed) res.close()
            if (!stm.isClosed()) stm.close()
        }
        selectSubClassManagers.call()
        //闭包查询流规则配置表
        def tStreamRuleDto = null
        def selectConfigs = {
            def tStreamRuleSelectBasic = "SELECT * FROM `tstream_rule`"
            def tStreamRuleSelectAlarm = "SELECT * FROM `tstream_rule_alarm`"
            def tStreamRuleSelectCalculation = "SELECT * FROM `tstream_rule_calculation`"
            def tStreamRuleSelectCollection = "SELECT * FROM `tstream_rule_collection`"
            def tStreamRuleSelectDist = "SELECT * FROM `tstream_rule_other_distributions`"
            def tStreamRuleSelectShoreBased = "SELECT * FROM `tstream_rule_shore_based_distributions`"
            def tStreamRuleSelectThinning = "SELECT * FROM `tstream_rule_thinning`"
            def tStreamRuleSelectWarehousing = "SELECT * FROM `tstream_rule_warehousing`"
            Statement stmBasic = con.createStatement()
            Statement stmAlarm = con.createStatement()
            Statement stmCalculation = con.createStatement()
            Statement stmCollection = con.createStatement()
            Statement stmDist = con.createStatement()
            Statement stmShoreBased = con.createStatement()
            Statement stmThinning = con.createStatement()
            Statement stmWarehousing = con.createStatement()
            ResultSet resBasic = stmBasic.executeQuery(tStreamRuleSelectBasic)
            ResultSet resAlarm = stmAlarm.executeQuery(tStreamRuleSelectAlarm)
            ResultSet resCalculation = stmCalculation.executeQuery(tStreamRuleSelectCalculation)
            ResultSet resCollection = stmCollection.executeQuery(tStreamRuleSelectCollection)
            ResultSet resDist = stmDist.executeQuery(tStreamRuleSelectDist)
            ResultSet resShoreBased = stmShoreBased.executeQuery(tStreamRuleSelectShoreBased)
            ResultSet resThinning = stmThinning.executeQuery(tStreamRuleSelectThinning)
            ResultSet resWarehousing = stmWarehousing.executeQuery(tStreamRuleSelectWarehousing)
            tStreamRuleDto = TStreamRuleDTO.createDto(resBasic, resAlarm, resCalculation, resCollection, resDist, resShoreBased, resThinning, resWarehousing)
            if (!resBasic.closed) resBasic.close()
            if (!stmBasic.isClosed()) stmBasic.close()
            if (!resAlarm.closed) resAlarm.close()
            if (!stmAlarm.isClosed()) stmAlarm.close()
            if (!resCalculation.closed) resCalculation.close()
            if (!stmCalculation.isClosed()) stmCalculation.close()
            if (!resCollection.closed) resCollection.close()
            if (!stmCollection.isClosed()) stmCollection.close()
            if (!resDist.closed) resDist.close()
            if (!stmDist.isClosed()) stmDist.close()
            if (!resShoreBased.closed) resShoreBased.close()
            if (!stmShoreBased.isClosed()) stmShoreBased.close()
            if (!resThinning.closed) resThinning.close()
            if (!stmThinning.isClosed()) stmThinning.close()
            if (!resWarehousing.closed) resWarehousing.close()
            if (!stmWarehousing.isClosed()) stmWarehousing.close()
        }
        selectConfigs.call()
        //获取所有路由名称并设置路由暂存,并暂存路由配置
        Map<String, NifiProcessorRoutesDTO> routeConf = [:]
        Map<Integer, NifiProcessorRoutesDTO> routeMap = [:]
        routesDto?.each {
            routeConf.put(it.getProperty('route_name') as String, it)
            routeMap.put(it.getProperty('route_id') as int, it)
        }
        createRelationships(routeConf.keySet() as List<String>)
        setRouteConf(routeConf)//路由表配置
        //设置属性暂存
        createParameters(attributesDto)
        //设置子脚本分组并暂存
        createSubClasses(subClassesDto, routeMap)
        //设置流规则暂存
        createTStreamRules(tStreamRuleDto)

        this.isInitialized.set(true)
        releaseConnection()
    }
    /**
     * 初始化子脚本并暂存至脚本实例仓库
     */
    void initScript() throws Exception {
        Map<String, GroovyObject> GroovyObjectMap = new HashMap<>()
        for (classDTOMap in subClasses.values()) {
            for (classDTOList in classDTOMap.values()) {
                for (classDTO in classDTOList) {
                    if ("A" == classDTO.status && !GroovyObjectMap.containsKey(classDTO.sub_script_name)) {
                        def path = classDTO.sub_full_path + classDTO.sub_script_name
                        GroovyClassLoader loader = new GroovyClassLoader()
                        Class aClass = loader.parseClass(new File(path))
                        GroovyObjectMap.put(classDTO.sub_script_name, aClass.newInstance() as GroovyObject)
                    }

                }
            }
        }
        scriptMap.putAll(GroovyObjectMap)
    }
}
/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorRoutesDTO {
    private Integer processor_id
    private Integer route_id
    private String route_name
    private String route_desc
    private String route_running_way
    private String status

    static List<NifiProcessorRoutesDTO> createDto(ResultSet res) throws Exception {
        try {
            def nifiProcessorRoutes = []
            while (res.next()) {
                NifiProcessorRoutesDTO dto = new NifiProcessorRoutesDTO()
                dto.setProperty('processor_id', res.getInt('processor_id'))
                dto.setProperty('route_id', res.getInt('route_id'))
                dto.setProperty('route_name', res.getString('route_name'))
                dto.setProperty('route_desc', res.getString('route_desc'))
                dto.setProperty('route_running_way', res.getString('route_running_way'))
                dto.setProperty('status', res.getString('status'))
                nifiProcessorRoutes.add(dto)
            }
            nifiProcessorRoutes
        } catch (Exception e) {
            throw new Exception("com.sdari.processor.testOn1.NifiProcessorRoutesDTO createDto has an error", e)
        }
    }
}


import groovy.json.JsonBuilder

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorSubClassDTO {
    private Integer processor_id
    private Integer route_id
    private String sub_full_path
    private String sub_script_name
    private String sub_running_way
    private Integer running_order
    private String status

    static List<NifiProcessorSubClassDTO> createDto(ResultSet res) throws Exception {
        try {
            def NifiProcessorSubClasses = []
            while (res.next()) {
                NifiProcessorSubClassDTO dto = new NifiProcessorSubClassDTO()
                /*dto.setProperty('processor_id', res.getInt('processor_id'))
                dto.setProperty('route_id', res.getInt('route_id'))
                dto.setProperty('sub_full_path', res.getString('sub_full_path'))
                dto.setProperty('sub_script_name', res.getString('sub_script_name'))
                dto.setProperty('sub_running_way', res.getString('sub_running_way'))
                dto.setProperty('running_order', res.getInt('running_order'))
                dto.setProperty('status', res.getString('status'))*/

                dto.processor_id = res.getInt('processor_id')
                dto.route_id = res.getInt('route_id')
                dto.sub_full_path = res.getString('sub_full_path')
                dto.sub_script_name = res.getString('sub_script_name')
                dto.sub_running_way = res.getString('sub_running_way')
                dto.running_order = res.getInt('running_order')
                dto.status = res.getString('status')
                NifiProcessorSubClasses.add(dto)
            }
            NifiProcessorSubClasses
        } catch (Exception e) {
            throw new Exception("com.sdari.processor.testOn1.NifiProcessorSubClassDTO createDto has an error", e)
        }
    }

    static JsonBuilder jsonBuilderDto(NifiProcessorSubClassDTO dto) throws Exception {
        try {
            def builder = new JsonBuilder()
            builder dto.collect(), { NifiProcessorSubClassDTO d ->
                processor_id d.processor_id
                route_id d.route_id
                sub_full_path d.sub_full_path
                sub_script_name d.sub_script_name
                sub_running_way d.sub_running_way
                running_order d.running_order
                status d.status
            }
            builder
        } catch (Exception e) {
            throw new Exception("com.sdari.processor.testOn1.NifiProcessorSubClassDTO jsonBuilderDto has an error", e)
        }
    }
}

import lombok.Data

import java.sql.ResultSet

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 17:38
 */
@Data
class NifiProcessorAttributesDTO {
    private Integer processor_id
    private String attribute_name
    private String attribute_value
    private String attribute_type
    private String status

    static List<NifiProcessorAttributesDTO> createDto(ResultSet res) throws Exception{
        try {
            def nifiProcessorAttributes = []
            while (res.next()) {
                NifiProcessorAttributesDTO dto = new NifiProcessorAttributesDTO()
                dto.setProperty('processor_id', res.getInt('processor_id'))
                dto.setProperty('attribute_name', res.getString('attribute_name'))
                dto.setProperty('attribute_value', res.getString('attribute_value'))
                dto.setProperty('attribute_type', res.getString('attribute_type'))
                dto.setProperty('status', res.getString('status'))
                nifiProcessorAttributes.add(dto)
            }
            nifiProcessorAttributes
        } catch (Exception e) {
            throw new Exception("com.sdari.processor.testOn1.NifiProcessorAttributesDTO createDto has an error", e)
        }

    }
}
