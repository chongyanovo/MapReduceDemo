package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class age extends UDF {
    public int evaluate(int age) {
        if (age < 20) {
            age = 30;
            return age;
        }
        return age;
    }
}
