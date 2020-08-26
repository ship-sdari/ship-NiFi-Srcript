import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import com.sdari.dto.manager.NifiProcessorSubClassDTO
import com.sdari.dto.manager.TStreamRuleDTO
import com.sdari.publicUtils.ProcessorComponentHelper
import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import org.apache.commons.net.imap.IMAP
import org.apache.nifi.dbcp.DBCPService
import sun.nio.ch.ThreadPool

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.*
import java.sql.Connection
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 10:44
 */
class RoutesTest extends GroovyTestCase {
    private Connection con
    private String url = 'jdbc:mysql://10.0.16.19:3306/groovy?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false&useLegacyDatetimeCode=false&useSSL=false&testOnBorrow=true'
    private String userName = 'appuser'
    private String password = 'Qgy@815133'

    //测试工具类
    void testRoutes() {
        DriverManager.setLoginTimeout(100)
        con = DriverManager.getConnection(url, userName, password)
        def pp = new ProcessorComponentHelper(1, con)
        pp.initComponent()//初始化
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
        println "返回结果" + tStreamRules?.size()
    }

    //测试类加载
    void testClassLoader() {
        final AtomicReference<JSONArray> dataList = new AtomicReference<>()
        dataList.set(JSONArray.parseArray('[]'))
        println dataList.get().getClass().canonicalName
        def pp = new ProcessorComponentHelper(1, null)
        /*GroovyObject object1 = pp.getClassInstanceByNameAndPath('NifiProcessorPublicDTO.groovy', 'E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\dto\\manager\\')
        object1.setProperty('sub_full_path', '第一次测试')
        println "返回结果 " + object1.getProperty('sub_full_path') as String
        println pp.aClasses.size()

        GroovyObject object2 = pp.getClassInstanceByNameAndPath('NifiProcessorPublicDTO.groovy', 'E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\dto\\manager\\')
        object2.setProperty('sub_full_path', '第二次测试')
        println "返回结果 " + object2.getProperty('sub_full_path') as String
        println pp.aClasses.size()*/

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

    void testSer(){
        TStreamRuleDTO dto = new TStreamRuleDTO()
        println(dto)
        def a = cloneTo(dto)
        println(a)
    }
}
