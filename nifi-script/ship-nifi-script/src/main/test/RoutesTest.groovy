import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.sdari.dto.manager.NifiProcessorSubClassDTO
import com.sdari.publicUtils.ProcessorComponentHelper
import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import org.apache.commons.net.imap.IMAP

import java.sql.Connection
import java.sql.ResultSet
import java.sql.Statement
import java.sql.*
import java.sql.Connection
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
        println('----------路由关系----------')
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
        /*    for (subClass in subClasses){
                def builder = NifiProcessorSubClassDTO.jsonBuilderDto(subClass)
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
        /*def pp = new ProcessorComponentHelper(1, null)
        GroovyObject object1 = pp.getClassInstanceByNameAndPath('NifiProcessorSubClassDTO.groovy', 'E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\dto\\manager\\')
        object1.setProperty('sub_full_path', '第一次测试')
        println "返回结果 " + object1.getProperty('sub_full_path') as String
        println pp.aClasses.size()

        GroovyObject object2 = pp.getClassInstanceByNameAndPath('NifiProcessorSubClassDTO.groovy', 'E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-srcipt\\src\\main\\groovy\\com\\sdari\\dto\\manager\\')
        object2.setProperty('sub_full_path', '第二次测试')
        println "返回结果 " + object2.getProperty('sub_full_path') as String
        println pp.aClasses.size()*/
    }


}
