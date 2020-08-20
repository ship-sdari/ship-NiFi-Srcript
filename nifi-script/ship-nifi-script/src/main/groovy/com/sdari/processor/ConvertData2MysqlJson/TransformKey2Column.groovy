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
    private static processorName
    private static routeId
    private static currentClassName

    TransformKey2Column(final ComponentLog logger, final String pid, final String pName, final String rid) {
        log = logger
        processorId = pid
        processorName = pName
        routeId = rid
        currentClassName = this.class.canonicalName
        log.info "[Processor_id = ${processorId} Processor_name = ${currentClassName} Route_id = ${routeId} Sub_class = ${currentClassName}] 初始化成功！"
    }

    static def calculation(params) {
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
            try {//详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                def tables = [:]
                def jsonAttributes = [:]
                //循环每一条数据中的每一个信号点
                for (dossKey in jsonDataFormer.keySet()) {
                    try {
                        final List<GroovyObject> warehousing = (rules?.get(jsonAttributesFormer.get('sid'))?.get(dossKey)?.getProperty('warehousing') as ArrayList)
                        if (null == warehousing || warehousing.size() == 0) {
                            throw new Exception('流规则中没有该信号点配置！')
                        }
                        for (warehousingDto in warehousing) {
                            final String tableName = ((warehousingDto as GroovyObject).getProperty('table_id') as String)
                            if (!tables.containsKey(tableName)) {
                                tables.put(tableName, new JSONObject())
                                //属性加入表名（包含后缀）、库名
                                JSONObject attribute = (jsonAttributesFormer.clone() as JSONObject)
                                attribute.put('tableName', tableName.concat(jsonAttributesFormer.getString('table.name.postfix') == null ? '' : jsonAttributesFormer.getString('table.name.postfix')))
                                attribute.put('databaseName', (processorConf.get('database.name.prefix') as String)?.concat(jsonAttributesFormer.getString('sid')))
                                jsonAttributes.put(tableName, attribute)
                            }
                            JSONObject table = (tables.get(tableName) as JSONObject)
                            table.put((warehousingDto as GroovyObject).getProperty('column_id') as String, jsonDataFormer.get(dossKey))
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${currentClassName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} 处理异常", e
                    }
                }
                //单条数据处理结束，放入返回仓库
                for (String tableName in tables.keySet()) {
                    dataListReturn.add(tables.get(tableName))
                    attributesListReturn.add(jsonAttributes.get(tableName))
                }
            } catch (Exception e) {
                log.error "[Processor_id = ${processorId} Processor_name = ${currentClassName} Route_id = ${routeId} Sub_class = ${currentClassName}] 处理单条数据时异常", e
            }

        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('data', dataListReturn)
        return returnMap
    }
}
