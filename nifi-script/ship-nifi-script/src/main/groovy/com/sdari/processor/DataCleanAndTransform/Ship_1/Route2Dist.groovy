package com.sdari.processor.DataCleanAndTransform.Ship_1

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 路由第三方
 */
class Route2Dist {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper

    Route2Dist(final ComponentLog logger, final int pid, final String pName, final int rid, GroovyObject pch) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        helper = pch
        log.info "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap)?.get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap)?.get('attributes') as ArrayList)
        final Map<String, Map<String, GroovyObject>> rules = (helper?.invokeMethod('getTStreamRules',null) as Map<String, Map<String, GroovyObject>>)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {
                //详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                //循环所有信号
                def doss_key = [:]
                String sid = jsonAttributesFormer.get('sid') as String
                for (String dossKey in jsonDataFormer.keySet()) {
                    try {
                        List dists = (rules?.get(sid)?.get(dossKey)?.getProperty('other_distributions') as List)
                        for (dist in dists){
                            if ('A' != dist['dist_status'] ||
                                    null == dist['dist_ip'] ||
                                    null == dist['dist_port'] ||
                                    doss_key.containsKey(dossKey)) continue
                            def value = jsonDataFormer.get(dossKey)
                            if (null == value) {
                                //
                            }else if (value instanceof BigDecimal) {
                                BigDecimal transfer = rules?.get(sid)?.get(dossKey)?.getProperty('transfer_factor') as BigDecimal
                                value = value * transfer
                                BigDecimal min = rules?.get(sid)?.get(dossKey)?.getProperty('value_min') as BigDecimal
                                BigDecimal max = rules?.get(sid)?.get(dossKey)?.getProperty('value_max') as BigDecimal
                                if ((null != min && value < min) || (null != max && value > max)){//量程清洗
                                    value = null
                                }
                            }
                            doss_key.put(dossKey, value)//写入值
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName} dossKey = ${dossKey}] 处理计算数据录入错误", e
                    }
                }
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(doss_key)
                attributesListReturn.add(jsonAttributesFormer)
            } catch (Exception e) {
                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 处理单条数据时异常", e
            }
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('data', dataListReturn)
        return returnMap
    }
}