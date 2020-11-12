package com.sdari.processor.SparsePoint

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import lombok.Data
import lombok.Getter
import org.apache.nifi.logging.ComponentLog
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/20 11:23
 * 子脚本模板
 */
class SparsePointDetail {
    private static ComponentLog log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static GroovyObject helper
    private static Map<String, DilutionDTO> dilutionDTOMap = new ConcurrentHashMap<>()

    SparsePointDetail(final ComponentLog logger, final int pid, final String pName, final int rid, GroovyObject pch) {
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
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, GroovyObject>> rules = (helper?.invokeMethod('getTStreamRules',null) as Map<String, Map<String, GroovyObject>>)
        final Map processorConf = (helper?.invokeMethod('getParameters',null) as Map)
        //循环list中的每一条数据
        for (int i = 0; i < (dataList as ArrayList).size(); i++) {
            try {
                final JSONObject jsonDataFormer = ((dataList as ArrayList).get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                final String coltime = (jsonAttributesFormer.getString('coltime'))
                final String sid = (jsonAttributesFormer.getString('sid'))
                final String shipCollectProtocol = jsonAttributesFormer.getString('ship.collect.protocol')
                final String shipCollectFreq = jsonAttributesFormer.getString('ship.collect.freq')
                for (String dossKey in jsonDataFormer.keySet()){
                    try {
                        final List thinning = rules?.get(sid)?.get(dossKey)?.getProperty('thinning') as List
                        if (null == thinning || thinning.size() == 0) {
                            throw new Exception('流规则中没有该信号点配置！')
                        }
                        for (thinningDto in thinning){
                            if (thinningDto['dilution_status'] != 'A') continue//抽稀状态关闭，跳过
                            Integer sparseRate = thinningDto['sparse_rate'] as Integer
                            if (null == sparseRate) continue//抽稀频率为空，跳过
                            Integer dilutionType = thinningDto['dilution_type'] as Integer
                            final String key = shipCollectProtocol + '/' + shipCollectFreq + '/' + sparseRate
                            //新增抽稀仓库
                            if (!dilutionDTOMap.containsKey(key)){
                                DilutionDTO dd = new DilutionDTO()
                                dd.sid = sid
                                dd.shipCollectProtocol = shipCollectProtocol
                                dd.shipCollectFreq = shipCollectFreq
                                dd.sparseRate = sparseRate
                                dd.startTime = coltime == null ? Instant.now().toEpochMilli() : Instant.parse(coltime).toEpochMilli()
                                dilutionDTOMap.put(key,dd)
                            }
                            if (!dilutionDTOMap.get(key).dossKeys.containsKey(dossKey)){
                                DilutionDTO.DossKeyDTO dkd = new DilutionDTO.DossKeyDTO()
                                dkd.dossKey = dossKey
                                dkd.dte = DilutionTypeEnum.valueToDilutionTypeEnum(dilutionType)
                                dkd.values = new ArrayList<>()
                                dilutionDTOMap.get(key).dossKeys.put(dossKey,dkd)
                            }
                            //输入抽稀仓库元素
                            dilutionDTOMap.get(key).load(coltime == null ? Instant.now().toEpochMilli() : Instant.parse(coltime).toEpochMilli(), dossKey, jsonDataFormer.get(dossKey))
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} 的数据输入过程有异常", e
                    }
                }
                //检查输出抽稀仓库结果
                for (DilutionDTO dd in dilutionDTOMap.values()){
                    JSONObject dilution
                    try {
                        Long now = coltime == null ? Instant.now().toEpochMilli() : Instant.parse(coltime).toEpochMilli()
                        dilution = dd.check(now, log)
                        if (null == dilution) continue //未达到抽稀状态
                        dataListReturn.add(dilution)
                        jsonAttributesFormer.put('table.name.postfix', processorConf.getOrDefault('table.name.postfix', '_total'))
                        attributesListReturn.add(jsonAttributesFormer)
                    }catch(Exception e){
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] 的数据检查和输出过程有异常", e
                    }
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
     * @author jinkaisong* date:2020-04-01 14:10
     */
    @Data
    static class DilutionDTO {
        private String sid
        private String shipCollectProtocol
        private String shipCollectFreq
        private Integer sparseRate
        private Long startTime//常变值
        /**
         * 暂存仓库
         */
        private Map<String, DossKeyDTO> dossKeys = new HashMap<>()

        synchronized JSONObject check(Long now, final ComponentLog log) {
            if ((now - startTime) / sparseRate >= 1) {//达到或超过抽稀频率
                JSONObject jo = new JSONObject()
                for (DossKeyDTO dd : dossKeys.values()) {
                    try {
                        switch (dd.dte) {
                            case DilutionTypeEnum.TYPE_LATEST:
                                jo.put(dd.dossKey, dd.values.get(dd.values.size() - 1))
                                break
                            case DilutionTypeEnum.TYPE_SUM:
                                double sum = dd.values.stream().filter(Objects.&nonNull).mapToDouble({ value -> (Double) value }).sum()
                                jo.put(dd.dossKey, BigDecimal.valueOf(sum))
                                break
                            case DilutionTypeEnum.TYPE_AVG:
                                OptionalDouble avg = dd.values.stream().filter(Objects.&nonNull).mapToDouble({ value -> (Double) value }).average()
                                BigDecimal b = avg.isPresent() ? BigDecimal.valueOf(avg.getAsDouble()) : null
                                jo.put(dd.dossKey, b)
                                break
                            default:
                                //默认取当前值
                                jo.put(dd.dossKey, dd.values.get(dd.values.size() - 1))
                        }
                        //清空暂存数据
                        dd.values.clear()
                    } catch (Exception e) {
                        log.error("抽稀处理有异常", e)
                        jo.put(dd.dossKey, null)
                    }
                }
                //消除开始时间
                this.startTime = null
                return jo
            } else {
                return null
            }
        }

        synchronized void load(long startTime, String dossKey, Object o) throws Exception {
            if (null == this.startTime){//没有起始时间，则赋值
                this.startTime = startTime
            }else if (this.startTime > startTime){//起始时间大于数据时间，说明来的数据是迟到数据，则舍弃这个数据
                return
            }
            //正常情况下正常输入
            DilutionTypeEnum type = dossKeys.get(dossKey).dte
            switch (type) {
                case DilutionTypeEnum.TYPE_SUM://求和
                case DilutionTypeEnum.TYPE_AVG://求平均
                    dossKeys.get(dossKey).values.add(o)
                    break
                case DilutionTypeEnum.TYPE_LATEST://最新值
                    dossKeys.get(dossKey).values.add(0, o)
                    break
                default:
                    throw new Exception("Unexpected value: ${type.type}")
            }
        }

        @Data
        static
        class DossKeyDTO {
            private DilutionTypeEnum dte
            private String dossKey
            private List<Object> values
        }
    }
    /**
     * @author jinkaisong@sdari.mail.com
     * @date 2020/4/08 17:18
     */
    enum DilutionTypeEnum {
        TYPE_SUM(1),//求累计
        TYPE_AVG(2),//求平均
        TYPE_LATEST(3),//最新值

        TYPE_OTHERS(0);//其它
        @Getter
        private int type

        private DilutionTypeEnum(int type) {
            this.type = type
        }

        static DilutionTypeEnum valueToDilutionTypeEnum(int value) {
            DilutionTypeEnum[] enums = values()
            for (DilutionTypeEnum anEnum : enums) {
                if (anEnum.type == value) {
                    return anEnum
                }
            }
            return TYPE_OTHERS
        }
    }
}
