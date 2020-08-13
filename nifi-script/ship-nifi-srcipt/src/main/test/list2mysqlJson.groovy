//import com.alibaba.fastjson.JSONArray
//import com.sdari.dto.manager.TStreamRuleDTO

public class list2mysqlJson {


    public static Object calculation(Map<String, Object> param) {
        if (null == param) return null
        //  JSONArray dataList = param.get("data") as JSONArray
        Map<String, String> attributes = param.get("attributes") as Map<String, String>
        attributes.put("test1", "ok")
        //  Map<String, Map<String, TStreamRuleDTO>> rules = param.get("rules") as Map<String, Map<String, TStreamRuleDTO>>
        param.put("attributes", attributes)
        return param
    }

}