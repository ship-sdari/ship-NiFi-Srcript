package com.sdari.processor.ConvertData2ESJson

import com.alibaba.fastjson.JSONArray
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

    TransformKey2Column(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
                def tables = [:]
                def jsonAttributes = [:]
                //循环每一条数据中的每一个信号点
                for (dossKey in jsonDataFormer.keySet()) {
                    try {
                        final JSONArray warehousing = (rules?.get(jsonAttributesFormer.get('sid'))?.get(dossKey)?.getJSONArray('warehousing'))
                        if (null == warehousing || warehousing.size() == 0) {
                            throw new Exception('流规则中没有该信号点配置！')
                        }
                        for (warehousingDto in warehousing) {
                            if ('A' != (warehousingDto as JSONObject).getString('write_status')) continue
                            final String tableName = (processorConf.getOrDefault('table.name.prefix', '') as String) + ((warehousingDto as JSONObject).getString('table_id')) + (jsonAttributesFormer.getOrDefault('table.name.postfix', '') as String)
                            if (!tables.containsKey(tableName)) {
                                //添加rowkey、upload_time、coltime
                                JSONObject tableJson = new JSONObject()
                                final String createTime = dateFormat(Instant.parse(jsonAttributesFormer.get('coltime') as String).toEpochMilli(), 'yyyyMMddHHmmssSSS', 'UTC')
                                final String rowkey = (jsonAttributesFormer.get('sid') as String)?.padLeft(4, '0') + createTime
                                tableJson.put('coltime', createTime)
                                tableJson.put('upload_time', dateFormat(Instant.now().toEpochMilli(), 'yyyy-MM-dd HH:mm:ss:SSS', 'UTC'))
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
                            JSONObject table = (tables.get(tableName) as JSONObject)
                            table.put((warehousingDto as JSONObject).getString('column_id'), jsonDataFormer.get(dossKey))
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
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
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
