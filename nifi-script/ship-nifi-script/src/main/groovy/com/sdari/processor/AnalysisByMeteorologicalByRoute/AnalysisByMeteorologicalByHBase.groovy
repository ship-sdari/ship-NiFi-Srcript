package com.sdari.processor.AnalysisByMeteorologicalByRoute

import com.alibaba.fastjson.JSONObject


/**
 * 气象文件处理脚本
 */
class AnalysisByMeteorologicalByHBase {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper

    //数据处理使用参数
    final static String TIME = 'time'
    final static String TABLE_NAME_OUT = 'table.name'
    final static String meteorological_time = "meteorological.time"
    //入ES使用参数
    final static String rowKey = "rowkey"
    //组件属性key
    final static String tables = 'hbase.table'
    final static String familyName = 'family.name'


    AnalysisByMeteorologicalByHBase(final def logger, final int pid, final String pName, final int rid, GroovyObject pch) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        helper = pch
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
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        //获取入HBase的表
        String table = processorConf.get(tables)
        //获取入HBase的类型
        String rowType = (processorConf.get(familyName) as String)

        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            final JSONObject JsonData = (dataList.get(i) as JSONObject)
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
            jsonAttributesFormer.put(TABLE_NAME_OUT, table)
            jsonAttributesFormer.put(familyName, rowType)

            JSONObject json = JsonData
            Double time = Double.valueOf(jsonAttributesFormer.get(meteorological_time) as String)

            Double jsonTime = (((json.get(TIME) as Double) - time) / 3600)
            json.put(TIME, jsonTime.longValue())

            jsonAttributesFormer.put(rowKey, json.get(rowKey))

            //单条数据处理结束，放入返回
            dataListReturn.add(json)
            attributesListReturn.add(jsonAttributesFormer)

        }
        //全部数据处理完毕，放入返回数据后返回
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