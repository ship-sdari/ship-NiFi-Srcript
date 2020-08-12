import com.sdari.dto.manager.NifiProcessorSubClassDTO
import com.sdari.publicUtils.ProcessorComponentHelper
import groovy.json.JsonOutput
import groovy.test.GroovyTestCase

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 10:44
 */
class RoutesTest extends GroovyTestCase {
    void testRoutes() {
        def pp = new ProcessorComponentHelper(1)
        pp.initComponent()//初始化
        println('----------路由关系----------')
        def relationships = pp.getRelationships()
        for (name in relationships.keySet()){
            println "返回结果 " + name + " : " + relationships.get(name)
            println "返回类型 " + name.class + " : " + relationships.get(name).class
        }
        println('----------配置属性----------')
        def attributes = pp.getParameters()
        for (name in attributes.keySet()){
            println "返回结果 " + name + " : " + attributes.get(name)
            println "返回类型 " + name.class + " : " + attributes.get(name).class
        }
        println('----------子脚本----------')
        def subClasses = pp.getSubClasses()
        for (subClass in subClasses){
            def builder = NifiProcessorSubClassDTO.jsonBuilderDto(subClass)
            println "返回结果 " + JsonOutput.toJson(builder.content[0])
            println "返回类型 " + subClass.class
        }
        println('----------流规则配置----------')
        def tStreamRules = pp.getTStreamRules()
        println "返回结果" + tStreamRules?.size()
    }
}
