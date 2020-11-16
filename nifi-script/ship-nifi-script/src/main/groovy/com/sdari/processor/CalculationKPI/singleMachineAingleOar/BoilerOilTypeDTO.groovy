package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单机单桨）
 * @kpiName: 锅炉用油类型
 * @author Liumouren
 * @date 2020-09-21 17:01:00
 */
class BoilerOilTypeDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    //指标名称
    private static kpiName = 'boiler_oil_type'
    //计算相关参数
    final static String SID = 'sid'

    BoilerOilTypeDTO(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
     * 锅炉用油类型的计算公式
     * 计算公式 ：0:重油；1：轻油'
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result = null;
            //锅炉用燃油
            BigDecimal boilerHFO = data.get("boil_use_hfo");
            //锅炉用柴油
            BigDecimal boilerMOD = data.get("boil_use_mdo");
            if(null==boilerHFO&&null==boilerMOD){
                log.debug("[${sid}] [${kpiName}] [${time}] 锅炉用燃油[${boilerHFO}] 锅炉用柴油[${boilerMOD}] result[${null}] ")
                return null;
            }
            //计算
            if (null!=boilerHFO&& boilerHFO == BigDecimal.ONE) {
                result = BigDecimal.valueOf(0)
            }else if (null!=boilerMOD&& boilerMOD == BigDecimal.ONE){
                result = BigDecimal.valueOf(1)
            }else if (null!=boilerMOD&& boilerMOD == BigDecimal.ZERO){
                result = BigDecimal.valueOf(0)
            }
            log.debug("[${sid}] [${kpiName}] [${time}] 锅炉用燃油[${boilerHFO}] 锅炉用柴油[${boilerMOD}] result[${result}] ")
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
