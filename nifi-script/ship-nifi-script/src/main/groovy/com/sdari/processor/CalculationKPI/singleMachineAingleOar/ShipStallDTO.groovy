package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单机单桨）
 * @kpiName: 船舶失速
 * @author Liumouren
 * @date 2020-09-22 11:03:00
 */
class ShipStallDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    //指标名称
    private static kpiName = 'ship_stall'
    //计算相关参数
    final static String SID = 'sid'

    ShipStallDTO(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        final Map shipConf = ((params as HashMap).get('shipConf') as HashMap)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            JSONObject json = new JSONObject()
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)

            String sid = jsonAttributesFormer.get(SID)
            String coltime = String.valueOf(Instant.now())
            //  String coltime = jsonAttributesFormer.get(COLTIME)
            //判断数据里是否 有 当前计算指标数据
            if (!JsonData.containsKey(kpiName)) {
                log.debug("[${sid}] [${kpiName}] [没有当前指标 计算所需的数据] result[${null}] ")
                json.put(kpiName, null)
            } else {
                Map<String, BigDecimal> maps = JsonData.get(kpiName) as Map<String, BigDecimal>
                BigDecimal result = calculationKpi((shipConf.get(sid) as Map<String, String>), maps, coltime, sid)
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
     * 船舶失速的计算公式
     * 计算公式 暂时为原代码的一致，并没有明确指出
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result = null
            BigDecimal dfWater = data.get("df");
            // 获取艉吃水
            BigDecimal daWater = data.get("da");
            BigDecimal type = null
            try {
                String a = configMap.get("DRAFT_X");
                type = new BigDecimal(a)
                // 获取艏吃水
            } catch (Exception ignored) {
                type = new BigDecimal(6.3)
            }
            dfWater = null == dfWater ? new BigDecimal(0) : dfWater;
            daWater = null == daWater ? new BigDecimal(0) : daWater;
            BigDecimal num = BigDecimal.valueOf(0.5) * dfWater.add(daWater);
            String r = null
            if (num > type) {
                r = "1";
            } else if (num <= type) {
                r = "0";
            }
            // 纵向对水速度
            BigDecimal vs = data.get("vs");
            // 轴功率(KW)
            BigDecimal shaftP = data.get("shaft_power");
            String dbParams
            try {
                //查询频率计算（根据船及key）
                if ("1" == r) {
                    dbParams=(configMap.get("WATER_SPEED_POWER_FULL_LOAD"));
                } else if ("0" == r) {
                    dbParams=(configMap.get("WATER_SPEED_POWER_BALLAST"));
                } else {
                    dbParams=null;
                }
            } catch (Exception e) {
                if ("1" == r) {
                    dbParams="6.038,0.0009128,-0.00000003986,0.000000000001108,-0.00000000000000001249";
                } else if ("0" == r) {
                    dbParams="6.425,0.001048,-0.00000004074,0.0000000000008114,-0.00000000000000000524";
                } else {
                    dbParams=null;
                }
                log.error("船舶失速查询配置有误，使用默认初始值:r[{}] 默认1[{6.038,0.0009128,-0.00000003986,0.000000000001108,-0.00000000000000001249}] " +
                        " 默认0[{6.425,0.001048,-0.00000004074,0.0000000000008114,-0.00000000000000000524}] 默认其他[{null}] 异常为", r, e);
            }
            if (null != dbParams && !dbParams.isEmpty() && shaftP !=null && shaftP >= BigDecimal.valueOf(3000) && vs != null) {
                try {
                    String[] split = dbParams.split(",");
                    result = BigDecimal.ZERO;
                    BigDecimal a0 = BigDecimal.valueOf(Double.parseDouble(split[0]));
                    BigDecimal a1 = BigDecimal.valueOf(Double.parseDouble(split[1]));
                    BigDecimal a2 = BigDecimal.valueOf(Double.parseDouble(split[2]));
                    BigDecimal a3 = BigDecimal.valueOf(Double.parseDouble(split[3]));
                    BigDecimal a4 = BigDecimal.valueOf(Double.parseDouble(split[4]));
                    BigDecimal x2 = shaftP * shaftP;
                    BigDecimal x3 = x2 * shaftP;
                    BigDecimal x4 = x3 * shaftP;
                    BigDecimal multiply1 = a1 * shaftP;
                    BigDecimal multiply2 = a2 * x2;
                    BigDecimal multiply3 = a3 * x3;
                    BigDecimal multiply4 = a4 * x4;
                    result = result.add(a0).add(multiply1).add(multiply2).add(multiply3).add(multiply4);
                    double value = vs.subtract(result).doubleValue();
                    log.debug("[${sid}] [${kpiName}] [${time}] dbParams[${dbParams}] shaftP[${shaftP}] vs{${vs}} result[${BigDecimal.valueOf(value).setScale(4, BigDecimal.ROUND_HALF_UP)}] ")
                    return BigDecimal.valueOf(value).setScale(4, BigDecimal.ROUND_HALF_UP)
                } catch (Exception e) {
                    log.error("[${sid}] [${kpiName}] [${time}] dbParams[${dbParams}] shaftP[${shaftP}] vs{${vs}} result[${result}] e:${e}")
                    return null;
                }
            } else {
                log.debug("[${sid}] [${kpiName}] [${time}] dbParams[${dbParams}] shaftP[${shaftP}] vs{${vs}} result[${result}] ")
                return null;
            }
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }
}
