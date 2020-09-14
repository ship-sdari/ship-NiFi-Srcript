package com.sdari.processor.AnalysisByMeteorologicalByRoute

import com.alibaba.fastjson.JSONObject
import org.joda.time.Instant


/**
 * 气象文件处理脚本
 */
class AnalysisByMeteorologicalByES {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName


    //数据处理使用参数
    final static String TABLE_NAME_OUT = 'table.name'
    //入ES使用参数
    final static String rowKey = "rowkey"
    //组件属性key
    final static String esType = "row.type"
    final static String esOperation = 'row.operation'
    final static String tables = 'es.table'
    //时间相关参数
    final static String upload_time = "upload_time"


    AnalysisByMeteorologicalByES(final def logger, final int pid, final String pName, final int rid) {
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
        String table = processorConf.get(tables)
        //获取入ES的类型
        String rowType = (processorConf.get(esType) as String)
        //ES的操作的类型
        String Operation = (processorConf.get(esOperation) as String)
        //循环list中的每一条数据
        log.info "循环list : 进入脚本方法"
        for (int i = 0; i < dataList.size(); i++) {
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
            jsonAttributesFormer.put(TABLE_NAME_OUT, table)
            jsonAttributesFormer.put(esType, rowType)
            jsonAttributesFormer.put(esOperation, Operation)

            JSONObject json = JsonData
            json.put(upload_time, upDate(String.valueOf(Instant.now())))
            jsonAttributesFormer.put(rowKey, json.get(rowKey))

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
     * String
     * 时间格式转换
     */
    static String upDate(String time) {
        return time.replace(".", ":").replace("T", " ").replace("Z", "")
    }

}