package com.sdari.processor.ConvertData2ESJson

import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

import java.text.SimpleDateFormat
import java.time.Instant

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将标准化数据转换为ES入库规范的数据格式
 */
class TransformKey2Column {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper

    TransformKey2Column(final ComponentLog logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
        final Map processorConf = (helper?.invokeMethod('getParameters',null) as Map)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {//详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                def tables = [:]
                def jsonAttributes = [:]
                //循环每一条数据中的每一个信号点
                for (dossKey in jsonDataFormer.keySet()) {
                    try {
                        final List warehousing = (rules?.get(jsonAttributesFormer.get('sid'))?.get(dossKey)?.getProperty('warehousing') as List)
                        if (null == warehousing || warehousing.size() == 0) {
                            throw new Exception('流规则中没有该信号点配置！')
                        }
                        for (warehousingDto in warehousing) {
                            try {
                                if ('A' != warehousingDto['write_status']) continue
                                final String tableName = (processorConf.getOrDefault('table.name.prefix', '') as String) + ((warehousingDto['table_id'])) + (jsonAttributesFormer.getOrDefault('table.name.postfix', '') as String)
                                if (!tables.containsKey(tableName)) {
                                    //添加rowkey、upload_time、coltime
                                    JSONObject tableJson = new JSONObject()
                                    final String timestamp = (Instant.parse(jsonAttributesFormer.get('coltime') as String).toEpochMilli() as String)
                                    final String createTime = dateFormat(Instant.parse(jsonAttributesFormer.get('coltime') as String).toEpochMilli(), 'yyyyMMddHHmmssSSS', 'UTC')
                                    final String rowkey = (jsonAttributesFormer.get('sid') as String)?.padLeft(4, '0') + createTime
                                    tableJson.put('coltime', timestamp)
                                    tableJson.put('upload_time', Instant.now().toEpochMilli() as String)
                                    tableJson.put('rowkey', rowkey)
                                    tables.put(tableName, tableJson)
                                    //属性加入表名（包含后缀）、库名
                                    JSONObject attribute = (jsonAttributesFormer.clone() as JSONObject)
                                    attribute.put('table.name', tableName)
                                    attribute.put('row.type', (processorConf.getOrDefault('row.type', '_doc') as String))
                                    attribute.put('row.operation', (processorConf.getOrDefault('row.operation', 'upsert') as String))
                                    attribute.put('rowkey', rowkey)
                                    jsonAttributes.put(tableName, attribute)
                                }
                                (tables.get(tableName) as JSONObject).put(warehousingDto['column_id'] as String, jsonDataFormer.get(dossKey))
                            } catch (Exception e) {
                                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} warehousing = ${warehousingDto['id']} 处理异常", e
                            }
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} 处理异常", e
                    }
                }
                //单条数据处理结束，放入返回仓库
                for (String tableName in tables.keySet()) {
                    dataListReturn.add(tables.get(tableName))
                    attributesListReturn.add(jsonAttributes.get(tableName))
                }
            } catch (Exception e) {
                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 处理单条数据时异常", e
            }

        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('data', dataListReturn)
        return returnMap
    }

    /**
     * 时间格式转换
     */
    static String dateFormat(long time, String time_type, String timeZone) {
        SimpleDateFormat t = new SimpleDateFormat(time_type)
        t.setTimeZone(TimeZone.getTimeZone(timeZone))
        return t.format(new Date(time))
    }
}
