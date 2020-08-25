package com.sdari.processor.DataAnalysis


import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.ArrayUtils
import org.apache.nifi.logging.ComponentLog

import java.text.SimpleDateFormat


/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将数据拆分路由到MySQL路由
 */
class analysisByMysql {
    private static log
    private static processorId
    private static processorName
    private static routeId
    private static currentClassName

    //新增
    final static String ADD = '0'
    //先删除后新增
    final static String DELETE_ADD = '1'
    //更新
    final static String UPDATE = '2'
    //删除
    final static String DELETE = '3'

    //数据处理使用参数
    final static String SID = 'sid'
    final static String STATUS = 'status'
    final static String DATA = 'data'
    final static String TABLE_NAME = 'tableName'
    final static String OPTION = 'option'
    final static String META = 'meta'
    //
    final String relationName = 'relationName'

    final static String[] FileTables = ['t_calculation', 't_alarm_history']

    final static String optionSTATUS = "optionSTATUS"
    final static String time_type = "yyyy-MM-dd HH:mm:ss"

    analysisByMysql(final ComponentLog logger, final int pid, final String pName, final int rid) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        log.info "[Processor_id = ${processorId} Processor_name = ${currentClassName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
        log.info "calculation : 进入脚本方法"
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, GroovyObject>> rules = ((params as HashMap).get('rules') as Map<String, Map<String, GroovyObject>>)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
            def metaMpa = JsonData.get(META) as JSONObject
            final String sid = metaMpa.get(SID)
            final String tableName = metaMpa.get(TABLE_NAME)
            final String status = metaMpa.get(STATUS)
            final String option = metaMpa.get(OPTION)
            JSONArray data = JsonData.get(DATA) as JSONArray
            jsonAttributesFormer.put(SID, sid)
            jsonAttributesFormer.put(STATUS, status)
            jsonAttributesFormer.put(OPTION, option)
            jsonAttributesFormer.put(TABLE_NAME, tableName)
            for (json in data) {
                json = json as JSONObject
                if (null == json) continue
                JSONObject jsonAttributesFormers = jsonAttributesFormer
                if (!ArrayUtils.contains(FileTables, tableName.toLowerCase())) {
                    String record_time = 'record_time'
                    if (json.containsKey(record_time) && json.get(record_time) != null) {
                        long time = Long.parseLong((json.get(record_time) as String)) as long
                        json.put(record_time, DateByFormat(time) as String)
                    }
                    String create_time = 'create_time'
                    if (json.containsKey(create_time) && json.get(create_time) != null) {
                        long time = Long.parseLong((json.get(create_time) as String)) as long
                        json.put(create_time, DateByFormat(time) as String)
                    }
                    String update_time = 'update_time'
                    if (json.containsKey(update_time) && json.get(update_time) != null) {
                        long time = Long.parseLong((json.get(update_time) as String)) as long
                        json.put(update_time, DateByFormat(time) as String)
                    }
                    switch (option) {
                        case ADD:
                            jsonAttributesFormers.put(optionSTATUS, ADD)
                            break
                        case DELETE_ADD:
                            jsonAttributesFormers.put(optionSTATUS, DELETE_ADD)
                            break
                        case UPDATE:
                            jsonAttributesFormers.put(optionSTATUS, UPDATE)
                            break
                        case DELETE:
                            jsonAttributesFormers.put(optionSTATUS, DELETE)
                            break
                        default:
                            jsonAttributesFormers.put(optionSTATUS, "null")
                    }
                    attributesListReturn.add(jsonAttributesFormers)
                    //单条数据处理结束，放入返回仓库
                    dataListReturn.add(json)
                }
            }
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('data', dataListReturn)
        return returnMap
    }
    /**
     * 时间格式转换
     */
    static String DateByFormat(long time) {
        return new SimpleDateFormat(time_type).format(new Date(time))
    }
}