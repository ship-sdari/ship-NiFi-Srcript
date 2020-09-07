package com.sdari.processor.DataAnalysis


import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.apache.commons.lang3.ArrayUtils
import org.apache.commons.lang3.StringUtils
import org.joda.time.Instant
import java.text.SimpleDateFormat

/**
 * @author wanghuaizhi@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将数据拆分路由到MySQL路由
 */
class analysisByES {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName


    //数据处理使用参数
    final static String SID = 'sid'
    final static String STATUS = 'status'
    final static String DATA = 'data'
    final static String TABLE_NAME = 'tableName'
    final static String OPTION = 'option'
    final static String META = 'meta'
    //入ES使用参数
    final static String isCompress = 'isCompress'
    final static String ROWTableName = "table.name"
    final static String rowKey = "rowkey"
    //组件属性key
    final static String esType = "row.type"
    final static String esOperation = 'row.operation'
    final static String tableNamePrefix = 'table.name.prefix'
    final static String tables = 'es.tables'
    //时间相关参数
    final static String time_type = "yyyy-MM-dd HH:mm:ss"
    final static String record_time = "record_time"
    final static String create_time = "create_time"
    final static String update_time = "update_time"
    final static String upload_time = "upload_time"

    analysisByES(final def logger, final int pid, final String pName, final int rid) {
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
        //获取入es的表
        String[] FileTables = (processorConf.get(tables) as String).split(',')
        //获取入ES的类型
        String rowType = (processorConf.get(esType) as String)
        //获取ES 表名前缀
        String tableNamePrefix = (processorConf.get(tableNamePrefix) as String)
        //ES的操作的类型
        String Operation = (processorConf.get(esOperation) as String)

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
            jsonAttributesFormer.put(esType, rowType)
            jsonAttributesFormer.put(esOperation, Operation)
            for (json in data) {
                if (null == json) continue
                json = json as JSONObject
                JSONObject jsonAttributesFormers = jsonAttributesFormer.clone() as JSONObject
                if (ArrayUtils.contains(FileTables, tableName.toLowerCase())) {
                    if (json.containsKey(record_time) && json.get(record_time) != null) {
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
                    String rowId = StringUtils.leftPad(sid, 4, "0").concat(json.get("id") as String)
                    json.put(rowKey, rowId)
                    json.put(upload_time, upDate(String.valueOf(Instant.now())))
                    jsonAttributesFormers.put(ROWTableName, (tableNamePrefix + tableName).toLowerCase())
                    jsonAttributesFormers.put(rowKey, rowId)
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
        SimpleDateFormat t= new SimpleDateFormat(time_type);
        t.setTimeZone(TimeZone.getTimeZone("UTC"))
        return t.format(new Date(time))
    }
    /**
     * String
     * 时间格式转换
     */
    static String upDate(String time) {
        return time.replace(".", ":").replace("T", " ").replace("Z", "")
    }
}