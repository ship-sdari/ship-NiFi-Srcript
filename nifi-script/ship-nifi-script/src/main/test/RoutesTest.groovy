import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import lombok.Data
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.ArrayUtils

import java.sql.*
import java.text.SimpleDateFormat
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 10:44
 */
class RoutesTest extends GroovyTestCase {
    private Connection con
    private String url = 'jdbc:mysql://10.0.16.20:3306/doss_e?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useLegacyDatetimeCode=false&useSSL=false&testOnBorrow=true'
    private String userName = 'appuser'
    private String password = 'Qgy@815133'
    private String sid = '1' as String

    @Data
    class WarehousingDTO {
        // 船id
        private Integer sid
        // DOSS系统key值
        private Integer doss_key
        //    入库库名-修改
        private String schema_id
        //数据表名
        private String table_id
        //列名
        private String column_id
        //数据类型
        private String data_type
        //启用状态-新增
        private String write_status
    }

    void testEs() {
        DriverManager.setLoginTimeout(10);//10s连接超时
        con = DriverManager.getConnection(url, userName, password);
        Statement t = con.createStatement()
        ResultSet resWarehousing = t.executeQuery("select  * from tstream_rule_warehousing")
        def createWarehousingDto = { dto, res ->
            dto.sid = res.getObject('sid') as Integer
            dto.doss_key = res.getObject('doss_key') as Integer
            dto.schema_id = res.getString('schema_id')
            dto.table_id = res.getString('table_id')
            dto.column_id = res.getString('column_id')
            dto.data_type = res.getString('data_type')
            dto.write_status = res.getString('write_status')
        }
        List<WarehousingDTO> warehousing = new ArrayList<>();
        //遍历入库表
        Map<String, String> a = new HashMap<>()
        while (resWarehousing.next()) {
            WarehousingDTO warehousingDto = new WarehousingDTO()
            createWarehousingDto.call(warehousingDto, resWarehousing)
            a.put(warehousingDto.table_id, "")
//            println("表名：" + warehousingDto.table_id +
//                    " 列名:" + warehousingDto.column_id +
//                    "数据类型:" + warehousingDto.data_type)
            warehousing.add(warehousingDto)
        }
        final String ld = '"'
        final String k = "\n{\n" +
                "\t\"mappings\": {\n" +
                "\t\t\"_doc\": {\n" +
                "\t\t\t\"properties\": {\"upload_time\": {\n" +
                "\t\t\t\t\t\"format\": \"yyyy-MM-dd HH:mm:ss:SSS\",\n" +
                "\t\t\t\t\t\"type\": \"date\"\n" +
                "\t\t\t\t},\"coltime\": {\n" +
                "\t\t\t\t\t\"format\": \"yyyyMMddHHmmssSSS\",\n" +
                "\t\t\t\t\t\"type\": \"date\"\n" +
                "\t\t\t\t},\"rowkey\": {\n" +
                "\t\t\t\t\t\"type\": \"keyword\"\n" +
                "\t\t\t\t}"
        final to = ","
        final n = "\n"
        final pu = "PUT "
        final de = "DELETE "
        final name1 = "xcloud_"
        final delDoc="/_doc"
        List<String> list = new ArrayList<>()
        for (String tableName : a.keySet()) {
            String tl= de+name1+tableName
      //  println(tl)
            String json = pu + name1 + tableName + k
            for (WarehousingDTO dto : warehousing) {
                if (tableName == dto.table_id) {

                    String dou = "double"
                    String a1 = to + ld
                    a1 = a1 + dto.column_id + ld + ": {" + n + "\t\t\t\t\t" + ld
                    a1 = a1 + "type" + ld + ":" + ld
                    a1 = a1 + (dto.data_type == "decimal(20,10)" ? dou : dto.data_type) + "\"\n\t\t\t\t}"
                    json = json.concat(a1)
                }
            }
            json = json.concat("}\n}\n}\n}\n")
            list.add(json)
            println(json)
        }

        resWarehousing.close()
        t.close()
        con.close()
    }

    void testHBase() {
        DriverManager.setLoginTimeout(10);//10s连接超时
        con = DriverManager.getConnection(url, userName, password);
        Statement t = con.createStatement()
        ResultSet resWarehousing = t.executeQuery("select  * from tstream_rule_warehousing")
        def createWarehousingDto = { dto, res ->
            dto.sid = res.getObject('sid') as Integer
            dto.doss_key = res.getObject('doss_key') as Integer
            dto.schema_id = res.getString('schema_id')
            dto.table_id = res.getString('table_id')
            dto.column_id = res.getString('column_id')
            dto.data_type = res.getString('data_type')
            dto.write_status = res.getString('write_status')
        }
        List<WarehousingDTO> warehousing = new ArrayList<>();
        //遍历入库表
        Map<String, String> a = new HashMap<>()
        while (resWarehousing.next()) {
            WarehousingDTO warehousingDto = new WarehousingDTO()
            createWarehousingDto.call(warehousingDto, resWarehousing)
            a.put(warehousingDto.table_id, "")
//            println("表名：" + warehousingDto.table_id +
//                    " 列名:" + warehousingDto.column_id +
//                    "数据类型:" + warehousingDto.data_type)
            warehousing.add(warehousingDto)
        }
        final String ld = "'"
        final String k = "\ncreate "
        final to = ","
        final info = "'INFO';"
        final name1 = "XCLOUD_"

        for (String tableName : a.keySet()) {
            String ak = k + ld
            String json = ak + name1 + tableName + ld + to + info
            println(json)
        }
        resWarehousing.close()
        t.close()
        con.close()
    }
    //测试工具类
    void testRoutes() {
//        DriverManager.setLoginTimeout(100)
//        con = DriverManager.getConnection(url, userName, password)
//        def pp = new ProcessorComponentHelper(1, con)
//        pp.initComponent(con)//初始化
        /*println('----------路由关系----------')
        def relationships = pp.getRelationships()
        for (name in relationships.keySet()) {
            println "返回结果 " + name + " : " + relationships.get(name)
            println "返回类型 " + name.class + " : " + relationships.get(name).class
        }
        println('----------配置属性----------')
        def attributes = pp.getParameters()
        for (name in attributes.keySet()) {
            println "返回结果 " + name + " : " + attributes.get(name)
            println "返回类型 " + name.class + " : " + attributes.get(name).class
        }
        println('----------子脚本----------')
        def subClasses = pp.getSubClasses()
        for (subClass in subClasses) {
            def builder = NifiProcessorPublicDTO.jsonBuilderDto(subClass)
            println "返回结果 " + JsonOutput.toJson(builder.content[0])
            println "返回类型 " + subClass.class
        }*/
        println('----------流规则配置----------')
        def tStreamRules = pp.getTStreamRules()
        println "返回结果" + tStreamRules
    }

    //测试类加载
    void testClassLoader() {
        final AtomicReference<JSONArray> dataList = new AtomicReference<>()
        dataList.set(JSONArray.parseArray('[]'))
        println dataList.get().getClass().canonicalName
//        def pp = new ProcessorComponentHelper(1, null)
        /*GroovyObject object1 = pp.getClassInstanceByNameAndPath('NifiProcessorPublicDTO.groovy', 'E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\dto\\manager\\')
        object1.setProperty('sub_full_path', '第一次测试')
        println "返回结果 " + object1.getProperty('sub_full_path') as String
        println pp.aClasses.size()

        GroovyObject object2 = pp.getClassInstanceByNameAndPath('NifiProcessorPublicDTO.groovy', 'E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\dto\\manager\\')
        object2.setProperty('sub_full_path', '第二次测试')
        println "返回结果 " + object2.getProperty('sub_full_path') as String
        println pp.aClasses.size()*/

    }


    void testClassLoader2() {
        InputStream inputStream = new FileInputStream(new File("E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-script\\src\\main\\groovy\\com\\sdari\\publicUtils\\ProcessorComponentHelper.groovy"))
        def processorComponentHelperText = IOUtils.toString(inputStream, 'UTF-8')
        GroovyObject pch
        //工具类实例化
        GroovyClassLoader classLoader = new GroovyClassLoader()
        Class aClass = classLoader.parseClass(processorComponentHelperText as String)
        pch = aClass.newInstance(2, null) as GroovyObject//有参构造
        pch.invokeMethod("initComponent", DriverManager.getConnection(url, userName, password))//相关公共配置实例查询
//        pch.invokeMethod("initScript", [null, null])
        //
        final def former = [(pch.getProperty("returnRules") as String)     : pch.getProperty('tStreamRules') as Map<String, Map<String, GroovyObject>>,
                            (pch.getProperty("returnAttributes") as String): [],
                            (pch.getProperty("returnParameters") as String): pch.getProperty('parameters') as Map,
                            (pch.getProperty("returnData") as String)      : []]
        /*final def former = [(pch.getProperty("returnRules") as String)     : ['1':['56890':['sid':1]]],
                            (pch.getProperty("returnAttributes") as String): [],
                            (pch.getProperty("returnParameters") as String): pch.getProperty('parameters') as Map,
                            (pch.getProperty("returnData") as String)      : []]*/
        //用来接收脚本返回的数据
//        def deep = (former.get('rules')  as Map<String, Map<String, GroovyObject>>).get('1')
//        println(JsonOutput.toJson((deep as Map<String, GroovyObject>).get('56890')))
//        println(JsonOutput.toJson(former))
        /*TestDTO ts = new TestDTO(sid: 1)
        def jsonOutput = new JsonOutput()
        def result = jsonOutput.toJson(ts)
        println(result)*/
//        Map returnMap = pch.invokeMethod("deepClone",JSONObject.parse(JsonOutput.toJson(former))) as Map
        Map returnMap = pch.invokeMethod("Clone", former) as Map
//        println(returnMap.get('rules'))
        println((returnMap.get('rules') as Map<String, Map<String, GroovyObject>>).get('1').get('56890'))
    }

    static def deepClone(def map) {
        String json = JSON.toJSONString(map)
        return JSON.parseObject(json, map.getClass() as Class<Object>)
    }

    //多线程测试
    void testThread() {
        def pool = []
        def damon1 = {
            println("t1 damon")
            Thread.sleep(10000)
            println("t1 damon")
        }
        def t1 = new Thread(damon1)
        pool.add(t1)
        //
        def damon2 = {
            def i = 0
            while (i < 10) {
                println("${i} : t2 damon")
                i++
                Thread.sleep(1000)
            }
        }
        def t2 = new Thread(damon2)
        pool.add(t2)
        for (Thread t in pool) {
            t.start()

        }
        while (pool.size() > 0) {
            Thread.sleep(1000)
            Iterator it = pool.iterator()
            while (it.hasNext()) {
                def t = it.next() as Thread
                println "t_id = ${t.id} t_status = ${t.alive}"
                if (!t.alive) {
                    it.remove()
                }
            }
        }
    }

    void testMap() {
        final def attributesMap = ["attribute1": "a1"] as Map<String, String>
        final AtomicReference<JSONArray> datas = new AtomicReference<>()
        datas.set(JSONArray.parseArray("[{\"key\": 1}]"))
        //调用脚本需要传的参数[attributesMap-> flowFile属性][data -> flowFile数据]
        def attributesList = []
        def dataList = []
        switch (datas.get().getClass().canonicalName) {
            case 'com.alibaba.fastjson.JSONObject':
                attributesList.add(attributesMap)
                dataList.add(datas.get())
                break
            case 'com.alibaba.fastjson.JSONArray':
                datas.get().each { o -> attributesList.add(attributesMap) }
                dataList.addAll datas.get()
                break
            default:
                throw new Exception("暂不支持处理当前所接收的数据类型：${datas.get().getClass().canonicalName}")
        }
        final HashMap former = ["rules"     : [:] as Map<String, Map<String, GroovyObject>>,
                                "attributes": attributesList,
                                "parameters": [:] as Map,
                                "data"      : dataList] as HashMap

        //循环路由名称 根据路由状态处理 [路由名称->路由实体]
        2.times {
            try {
                //用来接收脚本返回的数据
                Map returnMap = cloneTo(former)
                println("----------------------------")
                println "datas is ${JSONObject.toJSONString(datas.get(), SerializerFeature.WriteMapNullValue)}"
                println "former is ${JSONObject.toJSONString(former, SerializerFeature.WriteMapNullValue)}"
                println "returnMap is ${JSONObject.toJSONString(returnMap, SerializerFeature.WriteMapNullValue)}"
                //开始循环分脚本
                //执行详细脚本方法 [calculation ->脚本方法名] [objects -> 详细参数]
                returnMap = commit(returnMap)
                println("----------------------------")
                println "datas is ${JSONObject.toJSONString(datas.get(), SerializerFeature.WriteMapNullValue)}"
                println "former is ${JSONObject.toJSONString(former, SerializerFeature.WriteMapNullValue)}"
                println "returnMap is ${JSONObject.toJSONString(returnMap, SerializerFeature.WriteMapNullValue)}"
            } catch (Exception e) {
                e.printStackTrace()
            }
        }
    }

    void testTimeFormat() {
        Long now = Instant.now().toEpochMilli()
        println(now)
        SimpleDateFormat t1 = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss:SSS')
        t1.setTimeZone(TimeZone.getTimeZone("UTC"))
        SimpleDateFormat t2 = new SimpleDateFormat('yyyyMMddHHmmssSSS')
        t2.setTimeZone(TimeZone.getTimeZone("UTC"))
        println(t1.format(new java.util.Date(now)))
        println(t2.format(new java.util.Date(now)))

        println(sid.length() as String)
        println sid.padLeft(4, "0")

        println(this.class.canonicalName)
    }

    static Map commit(params) {
        if (null == params) return null
        def returnMap = [:]
        def dataListReturn = []
        def attributesListReturn = []
        final List<JSONObject> dataList = (params as HashMap).get('data') as ArrayList
        final List<JSONObject> attributesList = ((params as HashMap).get('attributes') as ArrayList)
        final Map<String, Map<String, GroovyObject>> rules = ((params as HashMap).get('rules') as Map<String, Map<String, GroovyObject>>)
        final Map processorConf = ((params as HashMap).get('parameters') as HashMap)
        //循环list中的每一条数据
        for (int i = 0; i < dataList.size(); i++) {
            try {//详细处理流程
                final JSONObject jsonDataFormer = (dataList.get(i) as JSONObject)
                final JSONObject jsonAttributesFormer = (attributesList.get(i) as JSONObject)
                def tables = [:]
                def jsonAttributes = [:]
                jsonDataFormer.put("key1", Instant.now())
                //单条数据处理结束，放入返回仓库
                dataListReturn.add(jsonDataFormer)
            } catch (Exception e) {
                e.printStackTrace()
            }

        }
        //全部数据处理完毕，放入返回数据后返回
        returnMap.put('rules', rules)
        returnMap.put('attributes', attributesListReturn)
        returnMap.put('parameters', processorConf)
        returnMap.put('data', dataListReturn)
        return returnMap
    }

    static <T> T cloneTo(T src) throws RuntimeException {
        ByteArrayOutputStream memoryBuffer = new ByteArrayOutputStream()
        ObjectOutputStream out = null
        ObjectInputStream inp = null
        T dist = null
        try {
            out = new ObjectOutputStream(memoryBuffer)
            out.writeObject(src)
            out.flush()
            inp = new ObjectInputStream(new ByteArrayInputStream(memoryBuffer.toByteArray()))
            dist = (T) inp.readObject()
        } catch (Exception e) {
            throw new RuntimeException(e)
        } finally {
            if (out != null) {
                try {
                    out.close()
                    out = null
                } catch (IOException e) {
                    throw new RuntimeException(e)
                }
            }
            if (inp != null) {
                try {
                    inp.close()
                    inp = null
                } catch (IOException e) {
                    throw new RuntimeException(e)
                }
            }
        }
        return dist
    }

    void testSer() {
        String a=" t_calculation  , t_alarm_history "
        List<String> te=[]
        te.add("t_calculation")
        te.add("t_alarm_history")
        String tableName="t_alarm_history"
        if (ArrayUtils.contains((te.toArray()), tableName.toLowerCase())) {
            println(tableName)
        }
    }
}
