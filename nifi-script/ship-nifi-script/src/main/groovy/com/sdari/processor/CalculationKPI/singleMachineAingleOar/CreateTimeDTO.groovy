package com.sdari.processor.CalculationKPI.singleMachineAingleOar


import com.alibaba.fastjson.JSONObject

import java.text.SimpleDateFormat

/**
 *
 * @type: （单机单桨）
 * @kpiName: 创建时间*
 */
class CreateTimeDTO {
    private static log
    private static processorId
    private static String processorName
    private static routeId
    private static String currentClassName

    //指标名称
    private static kpiName = 'create_time'
    //计算相关参数
    final static String SID = 'sid'
    final static String COLTIME = 'coltime'

    CreateTimeDTO(final def logger, final int pid, final String pName, final int rid) {
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
        final Map shipConf = ((params as HashMap).get('shipConf') as HashMap)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            JSONObject json = new JSONObject()
            final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
            String sid = jsonAttributesFormer.get(SID)
            String coltime = dateUp(sid, jsonAttributesFormer.get(COLTIME) as String)
            long time = dateUpByLong(sid, coltime);
            json.put(kpiName, time)
            //单条数据处理结束，放入返回
            dataListReturn.add(json)
            attributesListReturn.add(jsonAttributesFormer)
        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('shipConf', shipConf)
        returnMap.put('data', dataListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('attributes', attributesListReturn)
        return returnMap
    }

    /**
     * 时间格式化
     *
     * @param time Instant time
     * @return String
     */
    private static String dateUp(String sid, String time) {
        try {
            if (time.isEmpty()) {
                return null;
            }
            return time.replace("T", " ").replace("Z", "");
        } catch (Exception ignored) {
            log.error("[${sid}] [${kpiName}] [${time}] 时间格式化失败 e[${ignored}] ")
            return null;
        }
    }
    /**
     * 时间格式化
     *
     * @param time Instant time
     * @return String
     */
    private static Long dateUpByLong(String sid, String time) {
        try {
            if (time.isEmpty()) {
                return null;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf.parse(time).getTime()
        } catch (Exception ignored) {
            log.error("[${sid}]  [${kpiName}] [${time}] 时间格式化失败 e[${ignored}] ")
            return null;
        }
    }
}
