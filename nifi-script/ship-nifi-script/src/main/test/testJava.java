import java.math.BigDecimal;

/**
 * @author jinkaisong@sdari.mail.com
 * @date 2020/9/15 15:49
 */
public class testJava {
    public static void main(String[] args) {
        BigDecimal b = BigDecimal.ONE;
        Double d = Double.valueOf(String.valueOf(b));
        System.out.println(d);
    }
}
