package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单机单桨）
 * @kpiName: 纬度
 * @author Liumouren
 * @date 2020-09-21 18:47:00
 */
class LatDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'latitude'
    //计算相关参数
    final static String SID = 'sid'

    LatDTO(final def logger, final int pid, final String pName, final int rid) {
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
     * 纬度的计算公式
     * 计算公式 暂时为原代码的一致，并没有明确指出
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result = null;
            Integer LON_LAT_CALCULATION
            String a = configMap.get("LON_LAT_CALCULATION");
            if (a != null && !a.isEmpty()) {
                LON_LAT_CALCULATION = Integer.parseInt(a);
            } else {
                LON_LAT_CALCULATION = 1;
                log.error("纬度配置查询有误 null，使用默认初始值:[1] ");
            }
            Double dLat = 0;
            BigDecimal lat = data.get("lat");
            BigDecimal lon = data.get("lon");
            if (lat != null && lon != null) {
                if (LON_LAT_CALCULATION== 0) {
                    result = lat;
                } else {
                    dLat = lat.doubleValue();
                    lat = BigDecimal.valueOf((int) (dLat / 100)
                            + (dLat - (int) (dLat / 100) * 100) / 60d
                            + (dLat - (int) dLat) / 36);
                    result = lat.setScale(12, BigDecimal.ROUND_HALF_UP);
                }
            }
            log.debug("[${sid}] [${kpiName}] [${time}] lat[${lat}] lon[${lon}]  result[${result}] ")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }

    /**
     *
     * @param oilValue
     * @return
     */
    static BigDecimal oilRangeLimit(BigDecimal oilValue) {
        if (oilValue >= BigDecimal.valueOf(-2) && oilValue <= BigDecimal.valueOf(5)) {
            return oilValue;
        }
        return BigDecimal.valueOf(0);
    }

}
