package com.sdari.processor.testOn1
//import com.alibaba.fastjson.JSONArray
//import com.sdari.dto.manager.TStreamRuleDTO

public class list2sendShoreJson {


    public static Object calculation(List<Map<String, Object>> param) {
        if (null == param || param.size() == 0) return null
        //  JSONArray dataList = param.get("data") as JSONArray
        Map<String, String> attributes = param.get(0).get("attributes") as Map<String, String>
        attributes.put("test1", "ok")
        //  Map<String, Map<String, TStreamRuleDTO>> rules = param.get("rules") as Map<String, Map<String, TStreamRuleDTO>>
        param.get(0).put("attributes", attributes)
        return param
    }

}