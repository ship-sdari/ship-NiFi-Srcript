package com.sdari.processor.CalculationKPI.singleMachineAingleOar

import com.alibaba.fastjson.JSONObject
import groovy.sql.Sql
import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap

/**
 *
 * @type: （单桨单桨）
 * @kpiName:单位运输量co2
 */
class SysHealthDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    private static Sql con
    static final String conName = "con.name"
    //指标名称
    private static kpiName = 'sys_health'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'
    final static long timeSum = 60 * 60 * 60 * 24 * 1000
    static Map<String, BigDecimal> NumInMinute = new ConcurrentHashMap<>()

    SysHealthDTO(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        helper = pch
        currentClassName = this.class.canonicalName
        log.info "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final Map<String, Map<String, GroovyObject>> rules = (helper?.invokeMethod('getTStreamRules', null) as Map<String, Map<String, GroovyObject>>)
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        final Map shipConf = ((params as HashMap).get('shipConf') as HashMap)

        if (null == con) sqlINit()
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            JSONObject json = new JSONObject()
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)

            String sid = jsonAttributesFormer.get(SID)
            String coltime = jsonAttributesFormer.get(COLTIME)
            if (!NumInMinute.containsKey(sid)) {
                Map<String, GroovyObject> rule = rules.get(sid) as Map<String, GroovyObject>
                InstallNumInMinute(rule, sid, coltime)
            }
            //String coltime = String.valueOf(Instant.now())
            //判断数据里是否 有 当前计算指标数据
            if (!NumInMinute.containsKey(sid)) {
                log.debug("[${sid}] [${kpiName}] [没有当前sid 数据总条数] result[${null}] ")
                json.put(kpiName, null)
            } else {
                log.debug("[${sid}] [${kpiName}] [当前sid 数据总条数] sum [${NumInMinute.get(sid)}] ")
                BigDecimal result = calculationKpi(coltime, sid)
                json.put(kpiName, result)
            }
            //单条数据处理结束，放入返回
            dataListReturn.add(json)
            attributesListReturn.add(jsonAttributesFormer)
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('shipConf', shipConf)
        returnMap.put('data', dataListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('attributes', attributesListReturn)
        return returnMap
    }

    /**
     * 100-(报警条数/采集数据应有总条数)
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(final String time, final String sid) {
        BigDecimal result
        try {
            BigDecimal numInMinute = NumInMinute.get(sid) as BigDecimal
            if (null == numInMinute) return null
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            SimpleDateFormat dateFormatyMd = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
//          得到传入的colTime转换为正常格式的时间
            String colTime = dateUp(sid, time)
//          得到当前时间
            Date newDate = dateFormat.parse(colTime)
            String at = dateFormat.format(newDate)
//          得到两天前的当前时间
            Calendar cal = Calendar.getInstance()
            cal.setTime(newDate)
            cal.add(Calendar.DAY_OF_MONTH, -2)
            Date toDate = cal.getTime()
            //      得到今天零时到当前时间的分钟数
            int DateInt = DateByInt(sid, at)

            Integer sumNum = selectAbnormalAndDel(sid, dateFormatyMd.format(newDate), dateFormat.format(newDate), dateFormat.format(toDate))
            if (sumNum == null || sumNum == 0) {
                result = BigDecimal.valueOf(100d)
            } else {
                result = BigDecimal.valueOf(100).subtract(BigDecimal.valueOf(sumNum * 100).divide(BigDecimal.valueOf(numInMinute * DateInt), 2, BigDecimal.ROUND_HALF_UP))
            }
            log.debug("[${sid}] [${kpiName}] [${time}] 总数据条数[${numInMinute}]   报警条数[${sumNum}] result [${result}]")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }
    /**
     * 获取连接
     */
    static void sqlINit() {
        String conSqlName = (helper?.getProperty('parameters') as Map).get(conName) as String
        con = (helper?.invokeMethod('getMysqlPool', null) as Map)?.get(conSqlName) as Sql
    }
    /**
     * 配置查询
     * startTime : 00:00:00
     * endTime : new date()
     * delTIme : new Date(-2day())
     */
    static Integer selectAbnormalAndDel(String sid, String startTime, String endTime, String delTime) {
        try {
            final String sql_draft1 = "SELECT COUNT(id) from `t_ship_abnormal` where sid =".concat(sid).concat(" and create_time > '").concat(startTime).concat("' and create_time < '".concat(endTime).concat("' ;"))
            final String sql_draft2 = "delete from t_ship_abnormal WHERE sid =".concat(sid).concat(" and create_time < '".concat(delTime).concat("' ;"))
            int s = 0
            con.eachRow(sql_draft1) {
                res ->
                    s = res.getInt(1)
            }
//          删除两天前的当前时间的异常数据
            try {
                con.execute(sql_draft2)
                log.debug("[${sid}] [${kpiName}] [${startTime}] 两天前的异常数据删除成功！")
            } catch (Exception e) {
                log.debug("[${sid}] [${kpiName}] [${startTime}] 两天前的异常数据删除失败！e[${e}]")
            }
            return s
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${startTime}] 得到当前时间到今天凌晨的所有异常数据个数、两天前的异常数据删除出现异常 ，使用默认初始值:SumNum [0] 异常为：[${e}]")
        } finally {

        }
    }


    /**
     * 计算当前分钟
     * @param sid
     * @param date
     */
    private static Integer DateByInt(String sid, String date) {
        try {
            String a = GetDate(sid, date)
            date = dateUp(sid, date)
            if (date != null && a != null) {
                String b = date.split(" ")[1]
                String[] tre = b.split(":")
                return 60 * Integer.parseInt(tre[0]) + Integer.parseInt(tre[1])
            } else {
                return null
            }
        } catch (Exception ignored) {
            log.error("[${sid}] [${kpiName}] [${date}] 计算当前分钟失败 e[${ignored}] ")
            return null
        }
    }

    /**
     * 时间格式化
     *
     * @param time Instant time
     * @return String
     */
    private static String dateUp(String sid, String time) {
        try {
            if (time.isEmpty()) {
                return null
            }
            return time.replace("T", " ").replace("Z", "")
        } catch (Exception ignored) {
            log.error("[${sid}] [${kpiName}] [${time}] 时间格式化失败 e[${ignored}] ")
            return null
        }
    }

    /**
     * 获取今天日期
     *
     * @return String
     */
    static String GetDate(String sid, String time) {
        try {
            return time.split("T")[0] + " "
        } catch (Exception ignored) {
            log.error("[${sid}] [${kpiName}] [${time}] 时间格式化失败 e[${ignored}] ")
            return null
        }
    }


    /**
     * 根据配置表统计 当前sid  数据总数
     * @param rules
     */
    static void InstallNumInMinute(Map<String, GroovyObject> rules, String sid, String time) {
        try {
            Map<Long, Set<String>> data = new HashMap<>()
            if (rules != null) {
                for (GroovyObject rule : rules.values()) {

                    for (GroovyObject warehousing : rule.getProperty('collection') as List<GroovyObject>) {
                        Long freq = warehousing.getProperty('col_freq') as Long
                        if (data.containsKey(freq)) {
                            data.get(freq).add(warehousing.getProperty('doss_key') as String)
                        } else {
                            Set<String> ru = new HashSet<>()
                            ru.add(warehousing.getProperty('doss_key') as String)
                            data.put(freq, ru)
                        }
                    }
                }
            }

            if (data.keySet().size() > 0) {
                BigDecimal sum = 0
                data.keySet().forEach({ d ->
                    if (null != d) {
                        int size = data.get(d).size()
                        sum += (timeSum / d) * size
                    }
                })
                log.debug("[${sid}] [${kpiName}] sum [${sum}]")
                NumInMinute.put(sid, sum)
            }
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 统计数据总数失败 e[${e}] ")
        }
    }

}

