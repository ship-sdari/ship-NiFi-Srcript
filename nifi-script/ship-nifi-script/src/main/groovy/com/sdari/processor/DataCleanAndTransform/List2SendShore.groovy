package com.sdari.processor.DataCleanAndTransform

import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

import java.time.Instant

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将数据拆分路由到MySQL路由
 */
class List2SendShore {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    List2SendShore(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
            try {//详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                final String colTime = (Instant.ofEpochMilli(jsonDataFormer.getLong('time')) as String)
                jsonAttributesFormer.put('coltime', colTime)
                jsonDataFormer.remove('time')
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(jsonDataFormer)
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
}
