


class CalculationKPI {

    public static void main(String[] args) {
        Map<String, String> configMap = new HashMap<>()
        Map<String, BigDecimal> data = new HashMap<>()
        data.put("ge_fo1_total", new BigDecimal("14"))
        data.put("ge_fo2_total", new BigDecimal("13"))
        configMap.put("test", "test")

        /*
        GroovyClassLoader
         */
        GroovyClassLoader loader = new GroovyClassLoader();
        Class aClass = loader.parseClass(new File("F:\\IDEA\\nifi\\nifi-script\\nifi-script\\ship-nifi-srcipt\\src\\main\\script\\com\\sdari\\groovy\\kpi/AuxOilDTO.groovy"));
        try {
            Object[] objects = [configMap, data]
            GroovyObject instance = (GroovyObject) aClass.newInstance();
            print(instance.invokeMethod("calculation", objects))

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
