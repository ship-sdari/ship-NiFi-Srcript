import org.apache.commons.lang3.ArrayUtils
import org.apache.commons.lang3.StringUtils
import org.codehaus.groovy.runtime.InvokerHelper

class wTest {


    public static void main(String[] args) {
        final String[] filterTables = ['t_alarm_history', 't_calculation'];
        String srcTableName = "t_alarm_history";
        println "srcTableName.toLowerCase() :"+srcTableName.toLowerCase()
        if (!ArrayUtils.contains((filterTables ), srcTableName.toLowerCase())) {
            System.out.println("mysql");
        }else {
            System.out.println("es");
        }
        String srcTableName2 = "t_history";
        if (!ArrayUtils.contains(filterTables, srcTableName2.toLowerCase())) {
            System.out.println("mysql");
        }else {
            System.out.println("es");
        }
    }
}