package com.sdari.processor.analysisDataBySid

import com.alibaba.fastjson.JSONObject

import java.time.Instant
import org.apache.nifi.logging.ComponentLog

public class testByJSONObject {

    def log
    def id

    testByJSONObject(final ComponentLog logger, String pid) {
        log = logger
        id = pid
        log.info "[Processor_id = ${id} Processor_name = ${this.class}] 脚本 setLogger 执行成功，日志已设置完毕"

    }

    public static def calculation(def param) {
        if (null == param) return null
        List<LinkedHashMap> returnList = new ArrayList<>()
        for (paramMap in (param as List<LinkedHashMap>)) {
            //获取流文件属性
            Map<String, String> attributes = paramMap.get("attributes") as Map<String, String>
            def rules = paramMap.get("rules")
            //获取数据
            def dataList = paramMap.get("data")as JSONObject
            if (dataList.containsKey("time")) {
                attributes.put("coltime", String.valueOf(Instant.ofEpochMilli(dataList.getLongValue("time"))))
                dataList.remove("time")
            } else {
                attributes.put("gettime", String.valueOf(Instant.now()))
            }
            paramMap.put("rules", rules)//规则
            paramMap.put("attributes", attributes)//属性
            paramMap.put("data", dataList)
            returnList.add(paramMap)
        }
        return returnList
    }

}