package MyGrouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<OrderBean, Text, Text, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //int K = 1;
        int count = 0;
        for (Text i : values) {
            context.write(i, NullWritable.get());
            count++;
            if (count >= 1) {
                break;
            }
        }
    }
}
