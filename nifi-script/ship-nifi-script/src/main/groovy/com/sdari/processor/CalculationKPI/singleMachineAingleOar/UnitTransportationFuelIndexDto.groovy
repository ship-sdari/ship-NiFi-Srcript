package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单机单桨）
 * @kpiName: 单位距离燃料
 * @author Liumouren
 * @date 2020-09-22 15:41:00
 */
class UnitTransportationFuelIndexDto {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'unit_trantsportation_fuel'
    //计算相关参数
    final static String SID = 'sid'

    UnitTransportationFuelIndexDto(final def logger, final int pid, final String pName, final int rid) {
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
     * 单位距离燃料的计算公式
     * 计算公式 暂时为原代码的一致，并没有明确指出
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result
            Integer OIL_CALCULATION_TYPE
            BigDecimal SHIP_CARGO_VOLUME
            try {
                String a = configMap.get("OIL_CALCULATION_TYPE");
                if (a != null) {
                    OIL_CALCULATION_TYPE = Integer.parseInt(a);
                } else
                    log.error("单位运输量co2查询为 null，使用默认初始值:OIL_CALCULATION_TYPE[1] ");
                OIL_CALCULATION_TYPE = 1
            } catch (Exception e) {
                log.error("单位运输量co2查询有误，使用默认初始值:OIL_CALCULATION_TYPE[1]  异常为", e);
                OIL_CALCULATION_TYPE = 1
            }

            try {
                String a = configMap.get("CARGO_VOLUME");
                SHIP_CARGO_VOLUME = new BigDecimal(a);
            } catch (Exception e) {
                log.error("单位运输燃料 配置查询有误 ，使用默认初始值:SHIP_CARGO_VOLUME [390000] 异常为：", e);
                OIL_CALCULATION_TYPE = 1
                SHIP_CARGO_VOLUME = new BigDecimal(390000);
            }
            // 进口质量流量计流速（主机/辅机/锅炉）
            BigDecimal me_inRate;
            BigDecimal ge_inRate;
            BigDecimal blr_inRate;
            // 出口质量流量计流速（主机/辅机/锅炉）
            BigDecimal me_outRate;
            BigDecimal ge_outRate;
            BigDecimal blr_outRate;
            if (OIL_CALCULATION_TYPE == 0) {
                //60频率
                // 获取主机流入流量
                me_inRate = data.get("me_fo_in_total");
                // 获取主机流出流量
                me_outRate = data.get("me_fo_out_total");
                //60频率
                // 获取辅机流入流量
                ge_inRate = data.get("ge_fo_in_total");
                // 获取辅机流出流量
                ge_outRate = data.get("ge_fo_out_total");
                // 获取锅炉流入流量
                blr_inRate = data.get("boil_fo_in_total");
                // 获取锅炉流出流量
                blr_outRate = data.get("boil_fo_out_total");
            } else {
                // 获取主机流入流量
                me_inRate = data.get("me_fo_in_rate");
                // 获取主机流出流量
                me_outRate = data.get("me_fo_out_rate");
                // 获取辅机流入流量
                ge_inRate = data.get("ge_fo_in_rate");
                // 获取辅机流出流量
                ge_outRate = data.get("ge_fo_out_rate");
                // 获取锅炉流入流量
                blr_inRate = data.get("boil_fo_in_rate");
                // 获取锅炉流出流量
                blr_outRate = data.get("boil_fo_out_rate");
            }
            BigDecimal vg = data.get("vg");
            if (me_inRate == null || ge_inRate == null || blr_inRate == null || me_outRate == null || ge_outRate == null
                    || blr_outRate == null || null == vg || vg <= BigDecimal.valueOf(5)) {
                log.debug("[${sid}] [${kpiName}] [${time}] me_inRate[${me_inRate}] ge_inRate[${ge_inRate}] blr_inRate{${blr_inRate}} me_outRate[${me_outRate}] ge_outRate[${ge_outRate}] blr_outRate{${blr_outRate}} vg{${vg}} result[${null}] ")
                return null;
            }
//           计算指标
            if (vg > new BigDecimal(5)) {
                BigDecimal fint = me_inRate.add(ge_inRate).add(blr_inRate.divide(BigDecimal.valueOf(1000), 6, 4))
                        .subtract(me_outRate).subtract(ge_outRate)
                        .subtract(blr_outRate.divide(BigDecimal.valueOf(1000), 6, 4));
                BigDecimal multiply = SHIP_CARGO_VOLUME * vg;
                result = (fint * BigDecimal.valueOf(1000000)).divide(multiply, 6, 4);
            }
            log.debug("[${sid}] [${kpiName}] [${time}] me_inRate[${me_inRate}] ge_inRate[${ge_inRate}] blr_inRate{${blr_inRate}} me_outRate[${me_outRate}] ge_outRate[${ge_outRate}] blr_outRate{${blr_outRate}} vg{${vg}} result[${result}] ")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }
}
