package com.sdari.processor.CalculationKPI


import com.alibaba.fastjson.JSONObject
import com.sdari.vo.AmsInnerKeyVo
import org.joda.time.Instant


/**
 * @author wanghuaizhi@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将数据拆分路由到MySQL路由
 */
class WorkConditionIndexDto {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    WorkConditionIndexDto(final def logger, final int pid, final String pName, final int rid) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        log.info "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        log.info "calculation : 进入脚本方法"
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, JSONObject>> rules = ((params as HashMap).get('rules') as Map<String, Map<String, JSONObject>>)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)

            JSONObject json = JsonData
            BigDecimal a = calculationKpi(processorConf, json as Map<String, BigDecimal>)
            json.put('work_condition', a)
            //单条数据处理结束，放入返回
            dataListReturn.add(json)
            attributesListReturn.add(jsonAttributesFormer)

        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('data', dataListReturn)
        return returnMap
    }
    /**
     * 计算工况判定情况
     * 计算公式 吃水 = 0.5 * (艉吃水 + 艏吃水)
     *
     * @return
     * @param configMap
     * @param data
     */
    static BigDecimal calculationKpi(Map<String, String> configMap, Map<String, BigDecimal> data) {

        try {
            BigDecimal result = null
            // 获取艏吃水
            BigDecimal dfWater = data.get(AmsInnerKeyVo.DF)
            // 获取艉吃水
            BigDecimal daWater = data.get(AmsInnerKeyVo.DA)
            BigDecimal DRAFT_X = new BigDecimal(configMap.get('DRAFT_X'))
            if (daWater == null || dfWater == null) {
                //      log.debug("工况判定 [{}] [{}] [{}] 艏吃水[{}] 艉吃水[{}] num[{}] result[{}] 工况判定设定吃水[{}]", dfWater, daWater, null, null)
                return null;
            }

            BigDecimal num = BigDecimal.valueOf(0.5) * dfWater.add(daWater);
            if (num > DRAFT_X) {
                result = BigDecimal.valueOf(1);//满载
            } else if (num <= DRAFT_X) {
                result = BigDecimal.valueOf(0);//压载
            } else {//中载，暂时没有中载的情况
                result = BigDecimal.valueOf(2);
            }
            //    logger.debug("工况判定 [{}] [{}] [{}] 艏吃水[{}] 艉吃水[{}] num[{}] result[{}] 工况判定设定吃水[{}]", sid, indexGroup, Time_1, dfWater, daWater, num, result, type);
            return result;
        } catch (Exception ignored) {
            //      logger.debug("工况判定 [{}] [{}] [{}] 计算错误异常:{} value{}", sid, indexGroup, Time_1, e.getMessage(), getType());
            return null;
        }
    }


}