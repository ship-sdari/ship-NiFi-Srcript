package com.sdari.processor.DataAnalysis


import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.ArrayUtils
import org.apache.commons.lang3.StringUtils
import org.joda.time.Instant
import java.text.SimpleDateFormat

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将数据拆分路由到MySQL路由
 */
class analysisByES {
    private static log
    private static processorId
    private static processorName
    private static routeId
    private static currentClassName


    //数据处理使用参数
    final static String SID = 'sid'
    final static String STATUS = 'status'
    final static String DATA = 'data'
    final static String TABLE_NAME = 'tableName'
    final static String OPTION = 'option'
    final static String META = 'meta'
    //
    final static String tableNamePrefix = 'table_name_prefix'

    final static String isCompress = 'isCompress'

    final static String[] FileTables = ['t_calculation', 't_alarm_history']

    final static String table_name_prefix = "XCLOUD_"

    final static String time_type = "yyyy-MM-dd HH:mm:ss"

    analysisByES(final def logger, final int pid, final String pName, final int rid) {
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
            final boolean jsonIsCompress = jsonAttributesFormer.get(isCompress)
            JSONArray data = JsonData.get(DATA) as JSONArray
            jsonAttributesFormer.put(SID, sid)
            jsonAttributesFormer.put(STATUS, status)
            jsonAttributesFormer.put(OPTION, option)
            jsonAttributesFormer.put(TABLE_NAME, tableName)
            for (json in data) {
                if (null == json) continue
                json = json as JSONObject
                log.info"json:"+ JSONObject.toJSONString(json)
                JSONObject jsonAttributesFormers = jsonAttributesFormer
                if (ArrayUtils.contains(FileTables, tableName.toLowerCase())) {
                    String record_time = 'record_time'
                    if (json.containsKey(record_time) && json.get(record_time) != null) {
                        long time = Long.parseLong((json.get(record_time) as String))as long
                        json.put(record_time, DateByFormat(time)as String)
                    }
                    String create_time = 'create_time'
                    if (json.containsKey(create_time) && json.get(create_time) != null) {
                        long time = Long.parseLong((json.get(create_time) as String))as long
                        json.put(create_time, DateByFormat(time)as String)
                    }
                    String update_time = 'update_time'
                    if (json.containsKey(update_time) && json.get(update_time) != null) {
                        long time = Long.parseLong((json.get(update_time) as String))as long
                        json.put(update_time, DateByFormat(time)as String)
                    }
                    json.put("rowkey",
                            StringUtils.leftPad(sid, 4, "0")
                                    .concat(json.get(jsonIsCompress ? "id" : "create_time") as String))
                    json.put("upload_time", String.valueOf(Instant.now())
                            .replace(".", ":")
                            .replace("T", " ")
                            .replace("Z", ""))
                    jsonAttributesFormers.put(tableNamePrefix, (table_name_prefix + tableName).toLowerCase())
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