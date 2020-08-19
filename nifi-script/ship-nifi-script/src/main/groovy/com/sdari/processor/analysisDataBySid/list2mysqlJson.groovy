package com.sdari.processor.analysisDataBySid

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray

//import com.sdari.dto.manager.TStreamRuleDTO

//import com.alibaba.fastjson.JSONArray
//import com.sdari.dto.manager.TStreamRuleDTO

public class list2mysqlJson {


    public static def calculation(def param) {
        if (null == param) return null
        Map<String, String> attributes = (param as LinkedHashMap).get("attributes") as Map<String, String>
        String sid = attributes.get("sid")
        JSONArray dataList = (param as LinkedHashMap).get("data") as JSONArray
        JSONArray returnList = new ArrayList<>()
        //    Map<String, Map<String, TStreamRuleDTO>> rules = (param as LinkedHashMap).get("rules") as Map<String, Map<String, TStreamRuleDTO>>
        for (data in dataList) {
            JSONArray ja = new JSONArray()
            ja.add(data as JSON)
            returnList.add(ja as JSONArray)
        }
        (param as LinkedHashMap).put("data", returnList)
        return param
    }

}