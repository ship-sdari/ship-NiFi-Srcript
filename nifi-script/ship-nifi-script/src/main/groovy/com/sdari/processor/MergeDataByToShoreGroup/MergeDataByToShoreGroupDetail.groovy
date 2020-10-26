package com.sdari.processor.MergeDataByToShoreGroup

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import lombok.Data
import org.apache.nifi.logging.ComponentLog
import java.time.Instant

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 打包岸基分发数据子脚本
 */
class MergeDataByToShoreGroupDetail {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static Map<String, MergeGroupDTO> mergeGroupDTOMap
    private static List<MergeGroupDTO> history

    MergeDataByToShoreGroupDetail(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
        //特殊处理，接收主脚本中传入的合并仓库和历史仓库指针
        mergeGroupDTOMap = ((params as HashMap).get('mergeGroupDTOMap') as Map<String, MergeGroupDTO>)
        history = ((params as HashMap).get('history') as List<MergeGroupDTO>)
        if (null == mergeGroupDTOMap || null == history) throw new Exception("[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 接收指针失败！")
        //循环list中的每一条数据
        for (int i = 0; i < (dataList as ArrayList).size(); i++) {
            try {
                MergeGroupDTO merge
                final JSONObject jsonDataFormer = ((dataList as ArrayList).get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                final String toShoreGroup = jsonAttributesFormer.getString('shore.group')
                final String shipCollectProtocol = jsonAttributesFormer.getString('ship.collect.protocol')
                final String shipCollectFreq = jsonAttributesFormer.getString('ship.collect.freq')
                final String coltime = (jsonAttributesFormer.getString('coltime'))
                final String key = shipCollectProtocol + '/' + shipCollectFreq + '/' + toShoreGroup
                if (!mergeGroupDTOMap.containsKey(key)) {
                    merge = new MergeGroupDTO()
                    merge.sid = jsonAttributesFormer.getString('sid')
                    merge.shipCollectProtocol = shipCollectProtocol
                    merge.shipCollectFreq = shipCollectFreq
                    merge.shoreGroup = toShoreGroup
                    merge.shoreIp = jsonAttributesFormer.getString('shore.ip')
                    merge.shorePort = jsonAttributesFormer.getString('shore.port')
                    merge.shoreFreq = Integer.parseInt(jsonAttributesFormer.getString('shore.freq'))
                    merge.compressType = jsonAttributesFormer.getString('compress.type')
                    merge.shoreProtocol = jsonAttributesFormer.getString('shore.protocol')
                    mergeGroupDTOMap.put(key, merge)
                }
                merge = mergeGroupDTOMap.get(key)
                JSONObject merged = merge.addAndCheckOut(coltime, JSONObject.toJSONString(jsonDataFormer, SerializerFeature.WriteMapNullValue))
                if (null != merged) {//达到合并状态
                    String filename = String.join('-', [merge.sid, merge.shipCollectProtocol, merge.shipCollectFreq, merge.shoreGroup, merge.shoreIp, merge.shorePort, merge.shoreFreq as String, merge.compressType, merge.shoreProtocol, Instant.now().toEpochMilli() as String] as Iterable<? extends CharSequence>)
                    jsonAttributesFormer.put('file.name', filename)
                    dataListReturn.add(merged)
                    attributesListReturn.add(jsonAttributesFormer)
                } else {//未达到合并状态就检查历史仓库
                    if (history.size() > 0) {//有历史
                        Iterator its = history.iterator()
                        while (its.hasNext()) {
                            try {
                                MergeGroupDTO hisMerge = its.next() as MergeGroupDTO
                                JSONObject hisMerged = hisMerge.out()
                                JSONObject newAttributes = new JSONObject()
                                newAttributes.put('sid', hisMerge.sid)
                                newAttributes.put('ship.collect.protocol', hisMerge.shipCollectProtocol)
                                newAttributes.put('ship.collect.freq', hisMerge.shipCollectFreq)
                                newAttributes.put('shore.group', hisMerge.shoreGroup)
                                newAttributes.put('shore.ip', hisMerge.shoreIp)
                                newAttributes.put('shore.port', hisMerge.shorePort)
                                newAttributes.put('shore.freq', hisMerge.shoreFreq as String)
                                newAttributes.put('compress.type', hisMerge.compressType)
                                newAttributes.put('shore.protocol', hisMerge.shoreProtocol)
                                String filename = String.join('-', [hisMerge.sid, hisMerge.shipCollectProtocol, hisMerge.shipCollectFreq, hisMerge.shoreGroup, hisMerge.shoreIp, hisMerge.shorePort, hisMerge.shoreFreq as String, hisMerge.compressType, hisMerge.shoreProtocol, Instant.now().toEpochMilli() as String] as Iterable<? extends CharSequence>)
                                newAttributes.put('file.name', filename)
                                dataListReturn.add hisMerged
                                attributesListReturn.add newAttributes
                                its.remove()
                            } catch (Exception e) {
                                log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 的历史数据包路由有异常", e
                            }
                        }
                    }
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
     * 该实体类需要同主脚本中实体保持一致
     */
    @Data
    static class MergeGroupDTO {
        private String sid
        private String shipCollectProtocol
        private String shipCollectFreq
        private String shoreGroup
        private String shoreIp
        private String shorePort
        private Integer shoreFreq
        private String compressType
        private String shoreProtocol
        private JSONObject merge = new JSONObject(new TreeMap<String, Object>())

        private void push(String time, String data) throws Exception {
            merge.put(time, data)
        }

        private boolean check() throws Exception {
            List<String> times = new ArrayList<>(merge.keySet())
//            times = times.stream().sorted() as List<String>
            long gap = Instant.parse(times.max()).toEpochMilli() - Instant.parse(times.min()).toEpochMilli()
            return gap >= shoreFreq
        }

        synchronized private JSONObject out() throws Exception {
            SendMetaData sendMetaData = new SendMetaData()
            sendMetaData.meta.sid = this.sid
            sendMetaData.meta.shipCollectProtocol = this.shipCollectProtocol
            sendMetaData.meta.shipCollectFreq = this.shipCollectFreq
            sendMetaData.meta.compressType = this.compressType
            sendMetaData.data.addAll(merge.values())//输出仓库
            merge.clear()//清空仓库
            return JSONObject.toJSON(sendMetaData) as JSONObject
        }

        synchronized JSONObject addAndCheckOut(String time, String data) throws Exception {//同步类实例
            push(time, data)
            boolean isReturn = check()
            if (isReturn) {
                return out()
            } else {
                return null
            }
        }

        @Data
        class SendMetaData{
            private meta meta = new meta()
            private JSONArray data = new JSONArray()
            @Data
            class meta{
                private String sid
                private String shipCollectProtocol
                private String shipCollectFreq
                private String compressType
            }
        }
    }
}
