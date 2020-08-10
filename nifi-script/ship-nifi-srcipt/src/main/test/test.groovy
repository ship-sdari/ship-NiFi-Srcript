import groovy.json.JsonOutput


import java.util.concurrent.ConcurrentHashMap

class test {
    public static void main(String[] args) {
        GroovyClassLoader loader1 = new GroovyClassLoader();
        //File 需要绝对路径
        Class aClass2 = loader1.parseClass(new File("src\\com\\vo\\TStreamRuleDTO.groovy"));
        GroovyObject instance2 = (GroovyObject) aClass2.newInstance();
        Map<String, List<Object>> colgroups = new ConcurrentHashMap<>();
        Map<String, Set<String>> distgroups = new ConcurrentHashMap<>();
//        Object o = instance2.invokeMethod("t", null)
        def jsonOutput = new JsonOutput()
        def var = instance2.status
        println var

        List<Object> d = new ArrayList<>();
        Set<String> set = new HashSet<>()
        for (int i = 0; i < 5; i++) {
            d.add(instance2);
            set.add("aaa")
        }
        colgroups.put("kk", d);
        distgroups.put("ll", set)
        GroovyClassLoader loader = new GroovyClassLoader();//绝对路径
        Class aClass = loader.parseClass(new File("src/com/service/list2sendShoreJson.groovy"));
        GroovyObject instance = (GroovyObject) aClass.newInstance();
        Object[] objects = [colgroups, distgroups];
        instance.invokeMethod("init", objects)
        print(instance.invokeMethod("getColgroups", null))
        print(instance.invokeMethod("getDistgroups", null))
    }
}
