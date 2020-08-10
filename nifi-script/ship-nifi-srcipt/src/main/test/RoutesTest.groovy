import com.sdari.publicUtils.ProcessorComponentHelper
import groovy.test.GroovyTestCase

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/8/10 10:44
 */
class RoutesTest extends GroovyTestCase {
    void testRoutes() {
        def pp = new ProcessorComponentHelper()
        def names = ['success','sendShore']
        pp.createRelationships(names)
        def relationships = pp.getRelationships()
        for (name in relationships.keySet()){
            println "返回结果 " + name + " : " + relationships.get(name)
            println "返回类型 " + name.class + " : " + relationships.get(name).class
        }
    }
}
