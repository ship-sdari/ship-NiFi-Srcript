import org.apache.commons.lang3.StringUtils
import org.codehaus.groovy.runtime.InvokerHelper

class wTest {


    private final GroovyClassLoader classLoader = new GroovyClassLoader();

    public GroovyObject getClassByNameAndPath(String name, String path) {
        return classLoader.parseClass(new File(path + name)).newInstance() as GroovyObject
    }

    /**
     * 根据路径名称 加载class
     */
    private Script loadScript(String rule) {
        return loadScript(rule, new Binding())
    }

    private Script loadScript(String rule, Binding binding) throws Exception {
        Script script = null
        if (StringUtils.isEmpty(rule)) {
            return null;
        }
        try {
            Class ruleClazz = classLoader.parseClass(rule);
            if (ruleClazz != null) {
                return InvokerHelper.createScript(ruleClazz, binding)
            }
        } catch (Exception e) {
            throw new Exception("loadScript:", e)
        } finally {
            classLoader.clearCache()
        }
        return script
    }
}