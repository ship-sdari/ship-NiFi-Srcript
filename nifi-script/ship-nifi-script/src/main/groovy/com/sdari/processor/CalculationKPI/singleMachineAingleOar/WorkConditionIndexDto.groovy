package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单桨单桨）
 * @kpiName: 计算工况判定情况
 */
class WorkConditionIndexDto {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'work_condition'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'

    WorkConditionIndexDto(final def logger, final int pid, final String pName, final int rid) {
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
     * 计算工况判定情况
     * 计算公式 吃水 = 0.5 * (艉吃水 + 艏吃水)
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {

        try {
            BigDecimal result
            // 获取艏吃水
            BigDecimal dfWater = data.get('df')
            // 获取艉吃水
            BigDecimal daWater = data.get('da')
            BigDecimal DRAFT_X = BigDecimal.valueOf(configMap.get('DRAFT_X') as Double)
            if (daWater == null || dfWater == null || DRAFT_X == null) {
                log.debug("[${sid}] [${kpiName}] [${time}] df[${dfWater}] da[${daWater}] num[${null}] DRAFT_X{${DRAFT_X}} result[${null}] ")
                return null
            }

            BigDecimal num = BigDecimal.valueOf(0.5) * dfWater.add(daWater)
            if (num > DRAFT_X) {
                result = BigDecimal.valueOf(1)//满载
            } else if (num <= DRAFT_X) {
                result = BigDecimal.valueOf(0)//压载
            } else {//中载，暂时没有中载的情况
                result = BigDecimal.valueOf(2)
            }
            log.debug("[${sid}] [${kpiName}] [${time}] df[${dfWater}] da[${daWater}] num[${num}] DRAFT_X{${DRAFT_X}} result[${result}] ")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }


}