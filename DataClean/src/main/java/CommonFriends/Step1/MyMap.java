package CommonFriends.Step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(":");
        String str = line[0];
        String[] split = line[1].split(",");
        for (String i : split) {
            context.write(new Text(i), new Text(str));
        }
    }
}
