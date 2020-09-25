package com.sdari.processor.CalculationKPI


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单机单桨）
 * @kpiName: 单位距离co2排放
 * @author Liumouren
 * @date 2020-09-22 13:49:00
 */
class UnitDistanceCo2Dto {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'unit_distance_co2'
    //计算相关参数
    final static String SID = 'sid'
    static BigDecimal cf

    UnitDistanceCo2Dto(final def logger, final int pid, final String pName, final int rid) {
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
     * 单位距离co2排放的计算公式
     * 计算公式 暂时为原代码的一致，并没有明确指出
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result = null;
            Integer OIL_CALCULATION_TYPE
            String a = configMap.get("OIL_CALCULATION_TYPE");
            if (a != null) {
                OIL_CALCULATION_TYPE=Integer.parseInt(a);
            } else {
                log.error("单位距离co2排放 null，使用默认初始值:OIL_CALCULATION_TYPE[1] ");
                OIL_CALCULATION_TYPE=1
            }
            BigDecimal hfo = data.get("me_use_hfo");
            BigDecimal mdo = data.get("me_use_mdo");
            selectConfig1(configMap,hfo,mdo);
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
            if (me_inRate == null || ge_inRate == null || blr_inRate == null || me_outRate == null || ge_outRate == null
                    || blr_outRate == null || cf == null) {
                log.debug("[${sid}] [${kpiName}] [${time}] me_inRate[${me_inRate}] ge_inRate[${ge_inRate}] blr_inRate{${blr_inRate}} me_outRate[${me_outRate}] ge_outRate[${ge_outRate}] blr_outRate{${blr_outRate}} cf{${cf}} result[${null}] ")
                return null;
            }
            //对地航速
            BigDecimal vg = data.get("vg");
            if (vg != null) {
                int h = vg.compareTo(new BigDecimal(5));
                if (h > 0) {
                    //开始计算指标
                    BigDecimal fint;
                    try {
                        BigDecimal me = me_inRate.subtract(me_outRate);
                        BigDecimal ge = ge_inRate.subtract(ge_outRate);
                        BigDecimal blr = blr_inRate.subtract(blr_outRate).divide(BigDecimal.valueOf(1000f),4);
                        fint = me.add(ge).add(blr);
                    }catch (Exception ignored){
                        fint = BigDecimal.ZERO;
                    }
                    result = ((fint * BigDecimal.valueOf(1000000)) * getCf()).divide(vg, 2, BigDecimal.ROUND_HALF_UP);
                }
            }
            log.debug("[${sid}] [${kpiName}] [${time}] me_inRate[${me_inRate}] ge_inRate[${ge_inRate}] blr_inRate{${blr_inRate}} me_outRate[${me_outRate}] ge_outRate[${ge_outRate}] blr_outRate{${blr_outRate}} cf{${cf}} result[${result}] ")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }
    static void selectConfig1(Map<String, String> configMap,  BigDecimal hfo,BigDecimal mdo) {
        int k = 0;
        try {
            String key;
            if (null != hfo && "1" == hfo.toString()) {
                k = 1;
                key = "CF_HFO";
            } else if (BigDecimal.valueOf(1) == mdo) {
                k = 2;
                key = "CF_MDO";
            } else {
                k = 1;
                key = "CF_HFO";
            }
            String a = configMap.get(key);
            cf=new BigDecimal(a);
        } catch (Exception e) {
            e.printStackTrace();
            if (k == 1) {
                cf=new BigDecimal(3.114);
                log.error("单位距离co2排放 配置查询有误 ，使用默认初始值:cf [3.114] 异常为：", e);
            } else {
                cf=new BigDecimal(3.151);
                log.error("单位距离co2排放 配置查询有误 ，使用默认初始值:cf [3.151] 异常为：", e);
            }
        }
    }
}
