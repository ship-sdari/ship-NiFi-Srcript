package com.sdari.processor.CalculationKPI.singleMachineAingleOar

import com.alibaba.fastjson.JSONObject

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
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

    //指标名称
    private static kpiName = 'sys_health'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'
    final static String AbnormalKey = 'abnormalKey'
    final static long timeSum = 60 * 60 * 60 * 24 * 1000
    static Map<String, Long> NumInMinute = new ConcurrentHashMap<>()

    SysHealthDTO(final def logger, final int pid, final String pName, final int rid) {
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

        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            JSONObject json = new JSONObject()
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)

            String sid = jsonAttributesFormer.get(SID)
            String coltime = jsonAttributesFormer.get(COLTIME)
            if (!NumInMinute.containsKey(sid)) {
                Map<String, JSONObject> rule = rules.get(sid)
                InstallNumInMinute(rule, sid, coltime)
            }
            //String coltime = String.valueOf(Instant.now())
            //判断数据里是否 有 当前计算指标数据
            if (!NumInMinute.containsKey(SID)) {
                log.debug("[${sid}] [${kpiName}] [没有当前sid 数据总条数] result[${null}] ")
                json.put(kpiName, null)
            } else {
                Map<String, BigDecimal> maps = JsonData.get(kpiName) as Map<String, BigDecimal>
                BigDecimal result = calculationKpi(con, (shipConf.get(sid) as Map<String, String>), maps, coltime, sid)
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
     * 100-(报警条数/采集数据应有总条数)
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Connection con, Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        BigDecimal result = null;
        try {
            int CountvalueNumInMinute = configMap.get(AbnormalKey) as int
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat dateFormatyMd = new SimpleDateFormat("yyyy-MM-dd 00:00:00");
//          得到传入的Coltime转换为正常格式的时间
            String getColtime = dateUp(sid, time);
//          得到当前时间
            Date newDate = dateFormat.parse(getColtime);
            String at = dateFormat.format(newDate);
//          得到两天前的当前时间
            Calendar cal = Calendar.getInstance();
            cal.setTime(newDate);
            cal.add(Calendar.DAY_OF_MONTH, -2);
            Date toDate = cal.getTime();
            //      得到今天零时到当前时间的分钟数
            int DateInt = DateByInt(sid, at);

            Integer sumNum = selectAbnormalAndDel(con, sid, dateFormatyMd.format(newDate), dateFormat.format(newDate), dateFormat.format(toDate));
            if (sumNum == null || sumNum == 0) {
                result = BigDecimal.valueOf(100d);
            } else {
                result = BigDecimal.valueOf(100).subtract(BigDecimal.valueOf(sumNum * 100).divide(BigDecimal.valueOf(CountvalueNumInMinute * DateInt), 2, BigDecimal.ROUND_HALF_UP));
            }
            log.debug("[${sid}] [${kpiName}] [${time}] 总数据条数[${CountvalueNumInMinute}]   报警条数[${sumNum}] result [${result}]")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }

    /**
     * 配置查询
     * startTime : 00:00:00
     * endTime : new date()
     * delTIme : new Date(-2day())
     */
    static Integer selectAbnormalAndDel(Connection conn, String sid, String startTime, String endTime, String delTime) {
        Statement stmt1
        ResultSet rs1
        Statement stmt2
        try {
            final String sql_draft1 = "SELECT COUNT(id) from `t_ship_abnormal` where sid =".concat(sid).concat(" and create_time > '").concat(startTime).concat("' and create_time < '".concat(endTime).concat("' ;"));
            final String sql_draft2 = "delete from t_ship_abnormal WHERE sid =".concat(sid).concat(" and create_time < '".concat(delTime).concat("' ;"));
            int s = 0;
            stmt1 = conn.createStatement();
            //得到当前时间到今天凌晨的所有异常数据个数
            rs1 = stmt1.executeQuery(sql_draft1);
            while (rs1.next()) {
                s = (rs1.getInt(1));
            }
            if (!rs1.isClosed()) rs1.close();
            if (!stmt1.isClosed()) stmt1.close();
//          删除两天前的当前时间的异常数据
            stmt2 = conn.createStatement();
            //查询频率计算（根据船及key）
            try {
                stmt2.execute(sql_draft2);
                log.debug("[${sid}] [${kpiName}] [${startTime}] 两天前的异常数据删除成功！")
            } catch (Exception e) {
                log.debug("[${sid}] [${kpiName}] [${startTime}] 两天前的异常数据删除失败！e[${e}]")
            }
            if (!stmt2.isClosed()) stmt2.close();
            return s;
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${startTime}] 得到当前时间到今天凌晨的所有异常数据个数、两天前的异常数据删除出现异常 ，使用默认初始值:SumNum [0] 异常为：[${e}]");
        } finally {
            if (rs1 != null && !rs1.isClosed()) rs1.close()
            if (stmt1 != null && !stmt1.isClosed()) stmt1.close()
            if (stmt2 != null && !stmt2.isClosed()) stmt2.close()
        }
    }


    /**
     * 计算当前分钟
     * @param sid
     * @param date
     */
    private static Integer DateByInt(String sid, String date) {
        try {
            String a = GetDate(sid, date);
            date = dateUp(sid, date);
            if (date != null && a != null) {
                String b = date.split(" ")[1];
                String[] tre = b.split(":");
                return 60 * Integer.parseInt(tre[0]) + Integer.parseInt(tre[1]);
            } else {
                return null;
            }
        } catch (Exception ignored) {
            log.error("[${sid}] [${kpiName}] [${date}] 计算当前分钟失败 e[${ignored}] ")
            return null;
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
                return null;
            }
            return time.replace("T", " ").replace("Z", "");
        } catch (Exception ignored) {
            log.error("[${sid}] [${kpiName}] [${time}] 时间格式化失败 e[${ignored}] ")
            return null;
        }
    }

    /**
     * 获取今天日期
     *
     * @return String
     */
    static String GetDate(String sid, String time) {
        try {
            return time.split("T")[0] + " ";
        } catch (Exception ignored) {
            log.error("[${sid}] [${kpiName}] [${time}] 时间格式化失败 e[${ignored}] ")
            return null;
        }
    }


    /**
     * 根据配置表统计 当前sid  数据总数
     * @param rules
     */
    static void InstallNumInMinute(Map<String, JSONObject> rules, String sid, String time) {
        try {
            Map<Long, Set<String>> data = new HashMap<>()
            if (rules != null) {
                for (JSONObject rule : rules.values()) {
                    for (Map<String, String> warehousing : rule.get('collection') as List<Map<String, String>>) {
                        long freq = warehousing.get('col_freq') as long
                        if (data.containsKey(freq)) {
                            data.get(freq).add(warehousing.get('doss_key'))
                        } else {
                            Set<String> ru = new HashSet<>()
                            ru.add(warehousing.get('doss_key'))
                            data.put(freq, ru)
                        }
                    }
                }
            }

            if (data.size() > 0) {
                long sum = 0
                data.keySet().forEach({ d -> sum += (timeSum / d) * data.get(d).size() })
                NumInMinute.put(sid, sum)
            }
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 统计数据总数失败 e[${e}] ")
        }
    }

}

