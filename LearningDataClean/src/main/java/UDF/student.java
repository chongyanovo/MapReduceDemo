package UDF;

import org.apache.hadoop.hive.ql.exec.UDF;

public class student extends UDF {
    public double evaluate(double score) {
        if (score < 90) {
            score += 5;
            return score;
        }
        return score;
    }
}
