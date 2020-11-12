package com.sdari.processor.DataCleanAndTransform.Ship_1

import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 路由窗口报警数据支持
 */
class Route2AlarmWindow {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper

    Route2AlarmWindow(final ComponentLog logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, GroovyObject>> rules = (helper?.invokeMethod('getTStreamRules',null) as Map<String, Map<String, GroovyObject>>)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {
                //详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                //循环所有信号
                def alarmWindows = [:]
                String sid = jsonAttributesFormer.get('sid') as String
                for (String dossKey in jsonDataFormer.keySet()) {
                    Set<String> alarmWays = new HashSet<>()
                    def readAlarmWays = {
                        List alarms = (rules?.get(sid)?.get(dossKey)?.getProperty('alarm') as List)
                        if (null == alarms || alarms.size() == 0) return
                        for (alarm in alarms) {
                            alarmWays.add(alarm['alert_way'] as String)
                        }
                    }
                    readAlarmWays.call()
                    if (!alarmWays.contains('W') || alarmWindows.containsKey(dossKey)) continue//没有窗口报警方式
                    try {
                        def value = jsonDataFormer.get(dossKey)
                        if (null == value) continue
                        if (value instanceof BigDecimal) {
                            BigDecimal transfer = rules?.get(sid)?.get(dossKey)?.getProperty('transfer_factor') as BigDecimal
                            value = value * transfer
                            alarmWindows.put(dossKey, value)
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName} dossKey = ${dossKey}] 处理实时报警数据录入错误", e
                    }
                }
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(alarmWindows)
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
