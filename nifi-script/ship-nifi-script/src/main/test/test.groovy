import groovy.json.JsonOutput


import java.util.concurrent.ConcurrentHashMap

class test {
    public static void main(String[] args) {
       InputStream io = new FileInputStream(new File('E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-script\\src\\main\\groovy\\com\\sdari\\processor\\CompressData\\CompressData.groovy'))
        println(io.getClass().canonicalName)
    }
}
