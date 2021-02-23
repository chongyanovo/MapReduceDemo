package hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class agee extends UDF {
    public int evaluate(int agee) {
        if (agee < 20) {
            agee = 30;
            return agee;
        }
        return agee;
    }
}
