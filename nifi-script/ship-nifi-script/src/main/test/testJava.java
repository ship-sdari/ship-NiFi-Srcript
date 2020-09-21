import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/9/15 15:49
 */
public class testJava {
    public static void main(String[] args) {
        final String scriptPath = "E:\\CodeDevelopment\\ship-NiFi-srcript\\nifi-script\\ship-nifi-script\\src\\main\\groovy\\com\\sdari\\processor\\CompressData\\CompressData";
        try (FileInputStream scriptStream = new FileInputStream(scriptPath)) {
//            FileInputStream scriptStream = new FileInputStream(scriptPath);
            String s = IOUtils.toString(scriptStream, Charset.defaultCharset());
            System.out.println(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
