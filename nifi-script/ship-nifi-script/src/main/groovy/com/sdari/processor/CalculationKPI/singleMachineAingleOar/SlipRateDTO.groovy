package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单机单桨）
 * @kpiName: 对地滑失率
 * @author Liumouren
 * @date 2020-09-22 11:12:00
 */
class SlipRateDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    //指标名称
    private static kpiName = 'slip_rate'
    //计算相关参数
    final static String SID = 'sid'
    // 螺距,查询值
    final static BigDecimal PITCH = BigDecimal.valueOf(11.2d);

    SlipRateDTO(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
     * 辅对地滑失率的计算公式
     * 计算公式 暂时为原代码的一致，并没有明确指出
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result = null;
            // 获取对地航速
            BigDecimal Vsog = data.get("vg");
            // 获取转速
            BigDecimal n = data.get("me_ecs_speed");
            if (Vsog == null || n == null || n == BigDecimal.valueOf(0)) {
                log.debug("[${sid}] [${kpiName}] [${time}] 对地航速[${Vsog}] 获取转速[${n}]  result[${null}] ")
                return null;
            }
            result = slipperyDouble(Vsog, n, PITCH, "对地滑失率",BigDecimal.ZERO);
            if (Objects.requireNonNull(result) > BigDecimal.ONE){
                result = BigDecimal.ZERO;
            }
            log.debug("[${sid}] [${kpiName}] [${time}] 对地航速[${Vsog}] 获取转速[${n}]  result[${result}] ")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }
/**
 * @Title: slippery @Description: 滑失率计算 @param vsog 对地航速 @param n 主机转速 @param dp
 * 螺旋桨 @return Double
 */
    static BigDecimal slipperyDouble(BigDecimal vsog, BigDecimal n, BigDecimal dp, String name, BigDecimal defaultValue) {
        try {
            if (vsog == BigDecimal.valueOf(0) || n == BigDecimal.valueOf(0)) {
                return defaultValue;
            }
            BigDecimal bi1 = vsog * BigDecimal.valueOf(0.514 * 60);
            BigDecimal bi2 = n * dp;
            // 四舍五入
            return BigDecimal.valueOf(1).subtract(bi1.divide(bi2, 2, BigDecimal.ROUND_HALF_UP));
        } catch (Exception e) {
            e.printStackTrace();
            log.error("slipperyDouble 计算 异常为:{} 指标名 [{}]", e, name);
            return null;
        }
    }
}
