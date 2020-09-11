package com.sdari.processor.FormatDataByToShoreGroup

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

import java.text.SimpleDateFormat
import java.time.Instant

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将标准化数据转换为第三方岸基规范的数据格式
 */
class Split2ShoreGroup {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    Split2ShoreGroup(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
                def shoreGroups = [:]
                def jsonAttributes = [:]
                //循环每一条数据中的每一个信号点
                for (dossKey in jsonDataFormer.keySet()) {
                    try {
                        final JSONArray shore_based_distributions = (rules?.get(jsonAttributesFormer.get('sid'))?.get(dossKey)?.getJSONArray('shore_based_distributions'))
                        if (null == shore_based_distributions || shore_based_distributions.size() == 0) {
                            throw new Exception('流规则中没有该信号点配置！')
                        }
                        for (shoreBasedDto in shore_based_distributions) {
                            try {
                                final String shoreStatus = (shoreBasedDto as JSONObject).getString('to_shore_status')
                                if ('A' != shoreStatus) continue
                                final String toShoreGroup = ((shoreBasedDto as JSONObject).getString('to_shore_group'))
                                final String shipCollectProtocol = jsonAttributesFormer.getString('ship.collect.protocol')
                                final String shipCollectFreq = jsonAttributesFormer.getString('ship.collect.freq')
                                final String shoreIp = (shoreBasedDto as JSONObject).getString('to_shore_ip')
                                final String shorePort = (shoreBasedDto as JSONObject).getString('to_shore_port')
                                final String shoreFreq = (shoreBasedDto as JSONObject).getString('to_shore_freq')
                                final String compressType = (shoreBasedDto as JSONObject).getString('compress_type')
                                if (null == toShoreGroup || null == shipCollectProtocol || null == shipCollectFreq
                                        || null == shoreIp || null == shorePort || null == shoreFreq || null == compressType){
                                    throw new Exception('流规则配置不符合规范，请检查！')
                                }
                                //组合key 船基采集协议（来自船基文件属性）/船基采集频率（来自船基文件属性）/岸基分发组编号（来自岸基流规则配置）
                                final String key = shipCollectProtocol + '/' + shipCollectFreq + '/' + toShoreGroup
                                if (!shoreGroups.containsKey(key)) {
                                    //添加coltime
                                    JSONObject groupJson = new JSONObject(new TreeMap<String, Object>())
                                    final long coltime = Instant.parse(jsonAttributesFormer.get('coltime') as String).toEpochMilli()
                                    groupJson.put('coltime', coltime)
                                    shoreGroups.put(key, groupJson)
                                    //属性加入表名（包含后缀）、库名
                                    JSONObject attribute = (jsonAttributesFormer.clone() as JSONObject)
                                    attribute.put('shore.group', toShoreGroup)
                                    attribute.put('shore.protocol', (shoreBasedDto as JSONObject).getString('to_shore_protocol'))
                                    attribute.put('shore.ip', shoreIp)
                                    attribute.put('shore.port', shorePort)
                                    attribute.put('shore.freq', shoreFreq)
                                    attribute.put('compress.type', compressType)
                                    jsonAttributes.put(key, attribute)
                                }
                                (shoreGroups.get(key) as JSONObject).put(dossKey, jsonDataFormer.get(dossKey))
                            } catch (Exception e) {
                                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} shore_based_distributions = ${(shoreBasedDto as JSONObject).getString('id')} 处理异常", e
                            }
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} 处理异常", e
                    }
                }
                //单条数据处理结束，放入返回仓库
                for (String group in shoreGroups.keySet()) {
                    dataListReturn.add(shoreGroups.get(group))
                    attributesListReturn.add(jsonAttributes.get(group))
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
}
