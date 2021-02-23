package Reduce_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String first = "";
        String second = "";
        for (Text i : values) {
            if (i.toString().startsWith("p")) {
                first = i.toString();
            } else {
                second += i.toString();
            }
        }
        context.write(key, new Text(first + "\t" + second));
    }
}
