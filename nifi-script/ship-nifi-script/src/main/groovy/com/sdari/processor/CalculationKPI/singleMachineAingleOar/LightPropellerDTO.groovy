package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单机单桨）
 * @kpiName: 轻浆裕度
 * @author Liumouren
 * @date 2020-09-22 10:16:00
 */
class LightPropellerDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'light_propeller'
    //计算相关参数
    final static String SID = 'sid'

    LightPropellerDTO(final def logger, final int pid, final String pName, final int rid) {
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
     * 轻浆裕度的计算公式
     * 计算公式 暂时为原代码的一致，并没有明确指出
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result = BigDecimal.ZERO;
            // 轴功率
            BigDecimal dfWater = data.get("shaft_power");
            //主机转速
            BigDecimal shaftRpmStr = data.get("me_ecs_speed");
            //查询吃水（根据船）
            BigDecimal SMCR_P
            String a = configMap.get("SMCR_P");
            if(a!=null){
                SMCR_P=new BigDecimal(a);
            }else{
                SMCR_P=new BigDecimal(24200);
                log.error("轻桨裕度 配置查询有误，使用默认初始值: SMCR_P[24200]  SMCR_N[58]");
            }
            //查询吃水（根据船）
            BigDecimal SMCR_N
            String b = configMap.get("SMCR_N");
            if(b!=null){
                SMCR_N=new BigDecimal(b);
            }else{
                SMCR_N=new BigDecimal(58);
                log.error("轻桨裕度 配置查询有误，使用默认初始值: SMCR_P[24200]  SMCR_N[58]");
            }

            if (shaftRpmStr==null || dfWater==null || SMCR_N==null || SMCR_P==null) {
                log.debug("[${sid}] [${kpiName}] [${time}] 轴功率[${dfWater}] 主机转速[${shaftRpmStr}] SMCR_P{${SMCR_P}} SMCR_N{${SMCR_N}} result[${null}] ")
                return null;
            }
            try {
                BigDecimal c = SMCR_P.divide(SMCR_N.pow(3), 8, BigDecimal.ROUND_HALF_UP);
                double pow = Math.pow(dfWater.divide(c, 8, BigDecimal.ROUND_HALF_UP).doubleValue(), 1.0 / 3);// p/c的1/3次方
                result = result.add(shaftRpmStr.divide(BigDecimal.valueOf(pow), 8, BigDecimal.ROUND_HALF_UP))
                        .subtract(BigDecimal.ONE).setScale(3, BigDecimal.ROUND_HALF_UP);
            } catch (Exception ignored) {
                log.debug("[${sid}] [${kpiName}] [${time}] 轴功率[${dfWater}] 主机转速[${shaftRpmStr}] SMCR_P{${SMCR_P}} SMCR_N{${SMCR_N}} result[${result}] ")
            }
            log.debug("[${sid}] [${kpiName}] [${time}] 轴功率[${dfWater}] 主机转速[${shaftRpmStr}] SMCR_P{${SMCR_P}} SMCR_N{${SMCR_N}} result[${result}] ")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }
}
