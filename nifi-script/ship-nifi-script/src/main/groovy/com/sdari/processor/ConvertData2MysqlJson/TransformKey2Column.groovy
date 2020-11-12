package com.sdari.processor.ConvertData2MysqlJson

import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将标准化数据转换为MySQL入库规范的数据格式
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
        final Map<String, Map<String, GroovyObject>> rules = (helper?.invokeMethod('getTStreamRules', null) as Map<String, Map<String, GroovyObject>>)
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
                                //库名如果配置表中为空则组成库名
                                final String databaseName = (warehousingDto['schema_id'])
                                final String tableName = (warehousingDto['table_id']) + jsonAttributesFormer.getString('table.name.postfix') == null ? '' : jsonAttributesFormer.getString('table.name.postfix')
                                final String columnName = (warehousingDto['column_id'])
                                if (null == databaseName || null == tableName || null == columnName) {
                                    throw new Exception("配置不合法，库名、表名或列名为空！")
                                }
                                final String key = databaseName + '.' + tableName//组合key值，考虑进多库的情况
                                if (!tables.containsKey(key)) {
                                    tables.put(key, new JSONObject())
                                    //属性加入表名（包含后缀）、库名
                                    JSONObject attribute = (jsonAttributesFormer.clone() as JSONObject)
                                    attribute.put('table.name', tableName)
                                    attribute.put('database.name', databaseName)
                                    jsonAttributes.put(key, attribute)
                                }
                                (tables.get(key) as JSONObject).put(columnName, jsonDataFormer.get(dossKey))
                            } catch (Exception e) {
                                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} warehousing = ${warehousingDto['id']} 处理异常", e
                            }
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} 处理异常", e
                    }
                }
                //单条数据处理结束，放入返回仓库
                for (String key in tables.keySet()) {
                    dataListReturn.add(tables.get(key))
                    attributesListReturn.add(jsonAttributes.get(key))
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
}
