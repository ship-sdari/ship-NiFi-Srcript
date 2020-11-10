package com.sdari.processor.DataCleanAndTransform.Ship_1

import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 路由异常值
 */
class Route2Abnormal {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    Route2Abnormal(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {
                //详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                //循环信号做处理
                List<Abnormal> abnormalList = new ArrayList<>()
                String sid = jsonAttributesFormer.get('sid') as String
                String colTime = jsonAttributesFormer.get('coltime') as String
                boolean isAbnormal = false
                for (String dossKey in jsonDataFormer.keySet()){
                    try {
                        def value = jsonDataFormer.get(dossKey)
                        if (null == value) isAbnormal = true
                        if (value instanceof BigDecimal){
                            BigDecimal transfer = rules?.get(sid)?.get(dossKey)?.get('transfer_factor') as BigDecimal
                            BigDecimal min = rules?.get(sid)?.get(dossKey)?.get('value_min') as BigDecimal
                            BigDecimal max = rules?.get(sid)?.get(dossKey)?.get('value_max') as BigDecimal
                            value = value * transfer
                            isAbnormal = (value < min || value > max)
                        }
                        if (isAbnormal){
                            Abnormal abnormal = new Abnormal()
                            abnormal.sid = Integer.parseInt(sid)
                            abnormal.outer_key = rules?.get(sid)?.get(dossKey)?.get('orig_key')
                            abnormal.occur_time = colTime.replace('T', ' ').replace('Z', '')
                            abnormal.create_time = colTime.replace('T', ' ').replace('Z', '')
                            abnormalList.add(abnormal)
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName} dossKey = ${dossKey}] 处理异常值判断错误", e
                    }
                }
                //属性加入表名
                jsonAttributesFormer.put('table.name', 't_ship_abnormal')
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(abnormalList)
                attributesListReturn.add(jsonAttributesFormer)
            } catch (Exception e) {
                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 处理单条数据时异常", e
            }
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('data', dataListReturn)
        return returnMap
    }

    static class Abnormal{
        /**
         * 船id
         */
        private Integer sid
        /**
         * 异常信号点key
         */
        String outer_key
        /**
         * 异常时间
         */
        String occur_time
        /**
         * 创建时间
         */
        String create_time
    }
}
