package com.sdari.processor.CalculationKPI

import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
     *
     * @type: （单机单桨）
     * @kpiName: 锅炉油耗率
     * @author Liumouren
     * @date 2020-09-21 16:16:00
     */
class BoilerOilRateDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'boiler_oil_rate'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'

    final static BigDecimal slipperyValue = BigDecimal.valueOf(0.514 * 60)

    BoilerOilRateDTO(final def logger, final int pid, final String pName, final int rid) {
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
    returnMap.put('rules', rules)
    returnMap.put('shipConf', shipConf)
    returnMap.put('data', dataListReturn)
    returnMap.put('parameters', processorConf)
    returnMap.put('attributes', attributesListReturn)
    return returnMap
}

/**
 * 辅机油耗的计算公式
 * 计算公式为无
 *
 * @param configMap 相关系统配置
 * @param data 参与计算的信号值<innerKey,value></>
 */
static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
    try {
        BigDecimal result = null;
        BigDecimal inRate;
        BigDecimal outRate;
        BigDecimal ge1Power = data.get("me_ecs_power");
        BigDecimal realMeEcsPower = null;
        if (ge1Power != null) {
            String a = configMap.get("SMCR_P");
            if (a != null && !a.isEmpty()) {
                realMeEcsPower = ge1Power * (BigDecimal.valueOf(a as long) as BigInteger);
            } else {
                realMeEcsPower = ge1Power * BigDecimal.valueOf(24200);
            }

            realMeEcsPower = realMeEcsPower.divide(BigDecimal.valueOf(100f), 4).setScale(2, 4);
        }
        String a = configMap.get("OIL_CALCULATION_TYPE");
        Integer OIL_CALCULATION_TYPE
        if (a != null && !a.isEmpty()) {
            OIL_CALCULATION_TYPE = Integer.parseInt(a);
        } else {
            OIL_CALCULATION_TYPE = 1;
        }
        if (OIL_CALCULATION_TYPE == 0) {
            // 获取锅炉流入流量
            inRate = data.get("boil_fo_in_total");
            // 获取锅炉流出流量
            outRate = data.get("boil_fo_out_total");
        } else {
            // 获取锅炉流入流量
            inRate = data.get("boil_fo_in_rate");
            // 获取锅炉流出流量
            outRate = data.get("boil_fo_out_rate");
        }
        if (inRate == null || outRate == null) {
            log.debug("[${sid}] [${kpiName}] [${time}] 辅机流入流量[${inRate}] 辅机流出流量[${outRate}] 油耗计算方式{${OIL_CALCULATION_TYPE}} result[null] ")
            return null;
        }
        BigDecimal geOil = currentTimeOil(BigDecimal.valueOf(inRate.doubleValue() / 1000d), BigDecimal.valueOf(outRate.doubleValue() / 1000d));
        if (realMeEcsPower != null && realMeEcsPower != BigDecimal.valueOf(0d)) {
            result = (geOil.multiply(new BigDecimal(1000000)).divide((realMeEcsPower), 2, BigDecimal.ROUND_HALF_UP));
        }
        log.debug("[${sid}] [${kpiName}] [${time}] 辅机流入流量[${inRate}] 辅机流出流量[${outRate}] 油耗计算方式{${OIL_CALCULATION_TYPE}} result[${result}] ")
        return result
    } catch (Exception e) {
        log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
        return null
    }
}
    /**
     * currentTimeOil @Description: 当前时间点燃油消耗量，可用于计算小时流量消耗 @param oilIn
     * 进口质量流量计流速 @param oilOut 出口质量流量计流速 @return Double @author jiang @date
     */
    static BigDecimal currentTimeOil(BigDecimal oilIn, BigDecimal oilOut) {
        BigDecimal res;
        try {
            res = oilIn.subtract(oilOut);
            if (res < BigDecimal.valueOf(0)) {
                res = BigDecimal.valueOf(0d);
            }
        } catch (Exception ignored) {
            res = BigDecimal.valueOf(0d);
        }
        return res;
    }
}
