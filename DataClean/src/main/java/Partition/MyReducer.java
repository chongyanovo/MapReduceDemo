package Partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    public static enum Counter {
        MR_Reducer_COUNTER, MY_INPUT_RECORDES
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.getCounter(Counter.MR_Reducer_COUNTER).increment(1L);

        context.write(key, NullWritable.get());
    }
}
