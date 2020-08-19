package com.sdari.processor.analysisDataBySid

import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject

import java.time.Instant
import org.apache.nifi.logging.ComponentLog

public class list2mysqlJson {

    def log
    def id

    list2mysqlJson(final ComponentLog logger, String pid) {
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
            def dataList = paramMap.get("data")
            for (data in (dataList as JSONArray)) {
                LinkedHashMap map = new LinkedHashMap()
                def dataAttribute = attributes
                JSONObject jsonData = data as JSONObject
                if (jsonData.containsKey("time")) {
                    dataAttribute.put("coltime", String.valueOf(Instant.ofEpochMilli(jsonData.getLongValue("time"))))
                    jsonData.remove("time")
                } else {
                    dataAttribute.put("gettime", String.valueOf(Instant.now()))
                }
                map.put("rules", rules)//规则
                map.put("attributes", dataAttribute)//属性
                map.put("data", jsonData)
                returnList.add(map)
            }
        }
        return returnList
    }
}