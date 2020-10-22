package com.sdari.processor.DataAnalysis


import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.ArrayUtils
import org.apache.nifi.logging.ComponentLog

import java.text.SimpleDateFormat


/**
 * @author wanghuaizhi@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将数据拆分路由到MySQL路由
 */
class analysisByMysql {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

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
    final static String TABLE_NAME_OUT = 'table.name'
    final static String OPTION = 'option'
    final static String META = 'meta'
    //mysql 处理使用参数
    final static String tables = 'es.table.'
    //时间相关参数
    final static String time_type = "yyyy-MM-dd HH:mm:ss"
    final static String record_time = "record_time"
    final static String create_time = "create_time"
    final static String update_time = "update_time"
    final static String start_time = "start_time"
    final static String end_time = "end_time"

    analysisByMysql(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
        //获取入es的表
        List<String> FileTables = []
        for (String key : processorConf.keySet()) {
            if (key.contains(tables)) {
                FileTables.add((processorConf.get(key)) as String)
            }
        }    //循环list中的每一条数据
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
            jsonAttributesFormer.put(TABLE_NAME_OUT, tableName)
            for (json in data) {
                json = json as JSONObject
                if (null == json) continue
                JSONObject jsonAttributesFormers = jsonAttributesFormer.clone() as JSONObject
                if (!ArrayUtils.contains(FileTables.toArray(), tableName.toLowerCase())) {
                  /*  if (json.containsKey(record_time) && json.get(record_time) != null) {
                        long time = Long.parseLong((json.get(record_time) as String)) as long
                        json.put(record_time, DateByFormat(time) as String)
                    }
                    if (json.containsKey(create_time) && json.get(create_time) != null) {
                        long time = Long.parseLong((json.get(create_time) as String)) as long
                        json.put(create_time, DateByFormat(time) as String)
                    }
                    if (json.containsKey(update_time) && json.get(update_time) != null) {
                        long time = Long.parseLong((json.get(update_time) as String)) as long
                        json.put(update_time, DateByFormat(time) as String)
                    }
                    if (json.containsKey(start_time) && json.get(start_time) != null) {
                        long time = Long.parseLong((json.get(start_time) as String)) as long
                        json.put(start_time, DateByFormat(time) as String)
                    }
                    if (json.containsKey(end_time) && json.get(end_time) != null) {
                        long time = Long.parseLong((json.get(end_time) as String)) as long
                        json.put(end_time, DateByFormat(time) as String)
                    }*/
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