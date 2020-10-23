package com.sdari.processor.FormatDataByDistGroup

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import org.apache.nifi.logging.ComponentLog

import java.time.Instant

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 将标准化数据转换为第三方系统规范的数据格式
 */
class Split2DistGroup {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    Split2DistGroup(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
                def distGroups = [:]
                def jsonAttributes = [:]
                //循环每一条数据中的每一个信号点
                for (dossKey in jsonDataFormer.keySet()) {
                    try {
                        final JSONArray other_distributions = (rules?.get(jsonAttributesFormer.get('sid'))?.get(dossKey)?.getJSONArray('other_distributions'))
                        if (null == other_distributions || other_distributions.size() == 0) {
                            throw new Exception('流规则中没有该信号点配置！')
                        }
                        for (distDto in other_distributions) {
                            try {
                                final String distStatus = (distDto as JSONObject).getString('dist_status')
                                if ('A' != distStatus) continue
                                final String distGroup = ((distDto as JSONObject).getString('dist_group'))
                                final String shipCollectProtocol = jsonAttributesFormer.getString('ship.collect.protocol')
                                final String shipCollectFreq = jsonAttributesFormer.getString('ship.collect.freq')
                                final String distIp = (distDto as JSONObject).getString('dist_ip')
                                final String distPort = (distDto as JSONObject).getString('dist_port')
                                if (null == distGroup || null == shipCollectProtocol || null == shipCollectFreq
                                        || null == distIp || null == distPort){
                                    throw new Exception('流规则配置不符合规范，请检查！')
                                }
                                //组合key 船基采集协议（来自船基文件属性）/船基采集频率（来自船基文件属性）/第三方分发组编号（来自岸基流规则配置）
                                final String key = shipCollectProtocol + '/' + shipCollectFreq + '/' + distGroup
                                if (!distGroups.containsKey(key)) {
                                    //添加coltime
                                    JSONObject groupJson = new JSONObject(new TreeMap<String, Object>())
                                    final long coltime = Instant.parse(jsonAttributesFormer.get('coltime') as String).toEpochMilli()
                                    groupJson.put('coltime', coltime)
                                    groupJson.put('ship.collect.protocol', shipCollectProtocol)
                                    groupJson.put('ship.collect.freq', shipCollectFreq as Double)
                                    JSONArray array = new JSONArray()
                                    array.add(new JSONObject())
                                    groupJson.put('data', array)
                                    distGroups.put(key, groupJson)
                                    //属性加入表名（包含后缀）、库名
                                    JSONObject attribute = (jsonAttributesFormer.clone() as JSONObject)
                                    attribute.put('dist.group', distGroup)
                                    attribute.put('dist.protocol', (distDto as JSONObject).getString('dist_protocol'))
                                    attribute.put('dist.ip', distIp)
                                    attribute.put('dist.port', distPort)
                                    jsonAttributes.put(key, attribute)
                                }
                                (distGroups.get(key) as JSONObject)?.getJSONArray('data')?.getJSONObject(0)?.put(dossKey, jsonDataFormer.get(dossKey))
                            } catch (Exception e) {
                                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} shore_based_distributions = ${(distDto as JSONObject).getString('id')} 处理异常", e
                            }
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} 处理异常", e
                    }
                }
                //单条数据处理结束，放入返回仓库
                for (String group in distGroups.keySet()) {
                    dataListReturn.add(distGroups.get(group))
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
