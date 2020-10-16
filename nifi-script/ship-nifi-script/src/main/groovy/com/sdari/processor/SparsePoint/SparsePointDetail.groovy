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
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName
    private static Map<String, DilutionDTO> dilutionDTOMap = new ConcurrentHashMap<>()

    SparsePointDetail(final ComponentLog logger, final int pid, final String pName, final int rid) {
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
                        final JSONArray thinning = rules?.get(sid)?.get(dossKey)?.getJSONArray('thinning')
                        if (null == thinning || thinning.size() == 0) {
                            throw new Exception('流规则中没有该信号点配置！')
                        }
                        for (thinningDto in thinning){
                            if ((thinningDto as JSONObject).getString('dilution_status') != 'A') continue//抽稀状态关闭，跳过
                            Integer sparseRate = (thinningDto as JSONObject).getInteger('sparse_rate')
                            if (null == sparseRate) continue//抽稀频率为空，跳过
                            Integer dilutionType = (thinningDto as JSONObject).getInteger('dilution_type')
                            final String key = shipCollectProtocol + '/' + shipCollectFreq + '/' + sparseRate
                            //新增抽稀仓库
                            if (!dilutionDTOMap.containsKey(key)){
                                DilutionDTO dd = new DilutionDTO()
                                dd.sid = sid
                                dd.shipCollectProtocol = shipCollectProtocol
                                dd.shipCollectFreq = shipCollectFreq
                                dd.sparseRate = sparseRate
                                dd.startTime = coltime == null ? Instant.now().getEpochSecond() : Instant.parse(coltime).getEpochSecond()
                                DilutionDTO.DossKeyDTO dkd = new DilutionDTO.DossKeyDTO()
                                dkd.dossKey = dossKey
                                dkd.dte = DilutionTypeEnum.valueToDilutionTypeEnum(dilutionType)
                                dkd.values = new ArrayList<>()
                                dd.dossKeys.put(dossKey,dkd)
                                dilutionDTOMap.put(key,dd)
                            }
                            //输入抽稀仓库元素
                            log.info "dossKeys : ${JSONObject.toJSONString(dilutionDTOMap.get(key).dossKeys, SerializerFeature.WriteMapNullValue)}"
                            dilutionDTOMap.get(key).load(dossKey,jsonDataFormer.get(dossKey))
                        }
                    } catch (Exception e) {
                        log.error "[Processor_id = ${processorId} Processor_name = ${processorName} Route_id = ${routeId} Sub_class = ${currentClassName}] dosskey = ${dossKey} 的数据输入过程有异常", e
                    }
                }
                //检查输出抽稀仓库结果
                for (DilutionDTO dd in dilutionDTOMap.values()){
                    JSONObject dilution
                    try {
                        Long now = coltime == null ? Instant.now().getEpochSecond() : Instant.parse(coltime).getEpochSecond()
                        dilution = dd.check(now)
                        if (null == dilution) continue //未达到抽稀状态
                        dataListReturn.add(dilution)
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
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
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
        private Double sparseRate
        private Long startTime
        /**
         * 暂存仓库
         */
        private Map<String, DossKeyDTO> dossKeys = new HashMap<>()

        synchronized JSONObject check(Long now) {
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
                return jo
            } else {
                return null
            }
        }

        synchronized void load(String dossKey, Object o) throws Exception {
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
