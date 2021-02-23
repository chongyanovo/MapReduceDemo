package WenduSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Bean, Text, NullWritable, Text> {

    @Override
    protected void reduce(Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Counter In = context.getCounter("WenduSort_Reduce", "InNum");
        In.increment(1L);
        Counter Out = context.getCounter("WenduSort_Reduce", "OutNum");
        int K = 1;
        int count = 0;
        for (Text i : values) {
            count++;
            if (count > K) {
                break;
            } else {
                context.write(NullWritable.get(), i);
                Out.increment(1L);
            }
        }
    }
}
