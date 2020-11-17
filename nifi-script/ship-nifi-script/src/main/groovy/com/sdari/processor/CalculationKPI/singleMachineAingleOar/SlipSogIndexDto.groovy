package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.time.Instant

/**
 *
 * @type: （单桨单桨）
 * @kpiName: 表象滑式率
 */
class SlipSogIndexDto {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    //指标名称
    private static kpiName = 'slip_sog'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'

    final static BigDecimal slipperyValue = BigDecimal.valueOf(0.514 * 60)

    SlipSogIndexDto(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
            // String coltime = String.valueOf(Instant.now())
            String coltime = jsonAttributesFormer.get(COLTIME)
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
     * 表象滑失率率的计算公式
     * 计算公式为 1- 对地航速*0.514/ (np/60)
     * n 为 转速 mon_navstate NMS_1
     * p VLOC  = 8.51794m VLCC = 7.5730m
     *
     * @param configMap 相关系统配置
     * @param data 参与计算的信号值<innerKey,value></>
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data, final String time, final String sid) {
        try {
            BigDecimal result
            // 获取对地航速
            BigDecimal vg = data.get('vg')
            // 获取转速
            BigDecimal speed = data.get('me_ecs_speed')
            //螺距
            BigDecimal pitch = BigDecimal.valueOf(configMap.get('PITCH') as Double)
            if (vg == null || speed == null || speed == BigDecimal.ZERO || pitch == null) {
                log.debug("[${sid}] [${kpiName}] [${time}] 对地航速[${vg}] 转速[${speed}] 螺距[${pitch}] result[${null}] ")
                return null
            }
            result = slipperyDouble(vg, speed, pitch, BigDecimal.ONE, time, sid)
            if (Objects.requireNonNull(result) > BigDecimal.ONE) {
                result = BigDecimal.ZERO
            }
            log.debug("[${sid}] [${kpiName}] [${time}] vg[${vg}] me_ecs_speed[${speed}] PITCH{${pitch}} result[${result}] ")
            return result
        } catch (Exception e) {
            log.error("[${sid}] [${kpiName}] [${time}] 计算错误异常:${e} ")
            return null
        }
    }

    /**
     * 滑失率计算
     * @Title: slipperyDouble*
     *
     * @param vg 对地航速
     * @param speed 主机转速
     * @param pitch 螺旋桨
     * @return defaultValue
     */
    static BigDecimal slipperyDouble(BigDecimal vg, BigDecimal speed, BigDecimal pitch, BigDecimal defaultValue, final String time, final String sid) {
        try {
            if (vg == BigDecimal.ZERO || speed == BigDecimal.ZERO) {
                return defaultValue
            }
            BigDecimal bi1 = vg * slipperyValue
            BigDecimal bi2 = speed * pitch
            // 四舍五入
            return BigDecimal.ONE.subtract(bi1.divide(bi2, 2, BigDecimal.ROUND_HALF_UP))
        } catch (Exception e) {
            log.error("指标名 [${kpiName}][${sid}][${time}] slipperyDouble 计算 异常为:${e} ")
            return null
        }
    }
}
