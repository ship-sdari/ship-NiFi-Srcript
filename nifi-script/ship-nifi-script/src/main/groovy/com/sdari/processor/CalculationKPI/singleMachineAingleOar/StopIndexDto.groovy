package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.text.MessageFormat
import java.time.Instant

/**
 *
 * @type: （单桨单桨）
 * @kpiName: 航行状态
 */
class StopIndexDto {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'stop'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'
    //计算相关innerKey
    //主机转速
    final static String me_ecs_speed = 'me_ecs_speed'
    //计算相关 业务相关key
    final static String XRPMConf = 'XRPM'
    final static String YRPMConf = 'YRPM'

    //获取流配置相关
    final static String table_id = 'table_id'
    final static String column_id = 'table_id'
    final static String schema_id = 'schema_id'

    StopIndexDto(final def logger, final int pid, final String pName, final int rid) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        log.info "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, JSONObject>> rules = ((params as HashMap).get('rules') as Map<String, Map<String, JSONObject>>)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        final Map shipConf = ((params as HashMap).get('shipConf') as HashMap)
        Connection con = ((params as HashMap).get('con')) as Connection
        log.debug(" rules:[${JSONObject.toJSON(rules)}] [${kpiName}]  ")
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            JSONObject json = new JSONObject()
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)

            String sid = jsonAttributesFormer.get(SID)
            String coltime = jsonAttributesFormer.get(COLTIME)
            //判断数据里是否 有 当前计算指标数据
            if (!JsonData.containsKey(kpiName)) {
                log.debug("[${sid}] [${kpiName}] [没有当前指标 计算所需的数据] result[${null}] ")
                json.put(kpiName, null)
            } else {
                Map<String, BigDecimal> maps = JsonData.get(kpiName) as Map<String, BigDecimal>
                BigDecimal result = calculationKpi(con, (shipConf.get(sid) as Map<String, String>), maps, rules.get(sid), coltime, sid)
                json.put(kpiName, result)
            }
            //单条数据处理结束，放入返回
            dataListReturn.add(json)
            attributesListReturn.add(jsonAttributesFormer)
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('shipConf', shipConf)
        returnMap.put('data', dataListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('attributes', attributesListReturn)
        return returnMap
    }

    /**
     * 航行状态的计算公式
     * 计算 工况判别， 停泊/ 机动/ 定速航行
     * me_ecs_speed< XRPM  (主机转速) < 停泊标准 X)停泊
     * 主机转速标准差 > 机动航速标准 Y  机动航行
     * 主机转速标准差 < 机动航速标准 Y  匀速航行
     * 如果没有标准差就按照当前实时值计算
     *{主机转速 > 机动航速标准 Y 机动航行 否则 匀速航行}*
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Connection con, Map<String, String> configMap, Map<String, BigDecimal> data, Map<String, JSONObject> rules, final String time, final String sid) {
        Statement statement
        try {
            BigDecimal result
            statement = con.createStatement()
            Double meVar_double = null
            try {
                Map<String, String> stringMap = getDataByInner_key(me_ecs_speed, rules)
                meVar_double = selectVarMe(statement, time, stringMap)
            } catch (Exception e) {
                log.error("[${sid}] [${kpiName}] [${time}] selectVarMe:查询方差报错 ${e} ")
            }
            String xrpm = configMap.get(YRPMConf)
            BigDecimal XRPM = null
            if (xrpm != null && !xrpm.isEmpty()) {
                XRPM = new BigDecimal(xrpm)
            }
            BigDecimal YRPM = null
            String yrpm = configMap.get(XRPMConf)
            if (yrpm != null && !yrpm.isEmpty()) {
                YRPM = new BigDecimal(yrpm)
            }
            if (!statement.isClosed()) statement.close()
            // 获取转速
            BigDecimal n = data.get(me_ecs_speed)
            if (n == null || XRPM == null || YRPM == null) {
                log.debug("[${sid}] [${kpiName}] [${time}] 方差值[${meVar_double}]me_ecs_speed[${n}] XRPM{${XRPM}} YRPM{${YRPM}} result[${null}] ")
                return null
            }
            // 几个指标
            if (null != n && n < XRPM) {
                // 停泊
                result = BigDecimal.valueOf(0)
            } else if (null != meVar_double && BigDecimal.valueOf(meVar_double) > YRPM) {
                // 机动航行
                result = BigDecimal.valueOf(1)
            } else if (null != meVar_double && BigDecimal.valueOf(meVar_double) < YRPM) {
                // 匀速航行
                result = BigDecimal.valueOf(2)
            } else {//如果没有标准差就按照当前实时值计算
                if (null != n && n > YRPM) {
                    // 机动航行
                    result = BigDecimal.valueOf(1)
                } else {
                    // 匀速航行
                    result = BigDecimal.valueOf(2)
                }
            }
            log.debug("[${sid}] [${kpiName}] [${time}] 方差值[${meVar_double}]me_ecs_speed[${n}] XRPM{${XRPM}} YRPM{${YRPM}} result[${null}] ")
            return result
        } catch (Exception e) {
            if (statement != null && !statement.isClosed()) statement.close()
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }
    /**
     * 获取数据库数据
     * 并计算标准差
     * @param statement
     * @param getTime
     * @param rules
     */
    static Double selectVarMe(Statement statement, String getTime, Map<String, String> rules) throws Exception {
        Double standardDeviation = null
        String sql_data = MessageFormat.format("SELECT {0} FROM {1} WHERE coltime >=  ''{2}'' LIMIT 3600;",
                rules.get(column_id), rules.get(table_id),
                String.valueOf(Instant.parse(getTime).minusSeconds(3600))
                        .replace("T", " ").replace("Z", ""))

        List<Double> meSpeedList = new ArrayList<>()
        if (null != sql_data) {
            ResultSet res_data = statement.executeQuery(sql_data)
            while (res_data.next()) {
                if (null != res_data.getBigDecimal(1)) {
                    meSpeedList.add(res_data.getDouble(1))
                }
            }
            res_data.close()
            if (!res_data.isClosed()) res_data.close()
            if (meSpeedList.size() > 0) {
                Double[] arrays = new Double[meSpeedList.size()]
                standardDeviation = standardDeviationByMethod(meSpeedList.toArray(arrays))
            }
        }
        return standardDeviation
    }
    /**
     * standardDeviation
     * 计算标准差
     *
     * @param array Double[]
     * @return Double
     */
    static Double standardDeviationByMethod(Double[] array) {
        Double sum = 0d
        for (Double value : array) {
            sum += value // 求出数组的总和
        }
        double average = sum / array.length // 求出数组的平均数
        double total = 0
        for (Double aDouble : array) {
            total += (aDouble - average) * (aDouble - average) // 求出方差，如果要计算方差的话这一步就可以了
        }
        return Math.sqrt(total / array.length)
    }

    /**
     * 根据 inner_Key
     * 获取储存的库名,表名,字段名
     */
    static Map<String, String> getDataByInner_key(String inner_key, Map<String, JSONObject> rules) {
        if (rules != null) {
            rules.values().forEach({ rule ->
                if (inner_key == rule.get('inner_key')) {
                    Map<String, String> data = new HashMap<>()
                    List<JSONObject> warehousing = rule.get('warehousing') as List<JSONObject>
                    warehousing.forEach({ w ->
                        data.put(schema_id, w.get(schema_id) as String)
                        data.put(table_id, w.get(table_id) as String)
                        data.put(column_id, w.get(column_id) as String)
                        return data
                    })
                }
            })
        }
        return null
    }
}

