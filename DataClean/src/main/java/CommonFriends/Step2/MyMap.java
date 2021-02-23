package CommonFriends.Step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

public class MyMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String str = line[1];
        String[] split = line[0].toString().split("-");
        Arrays.sort(split);
        for (int i = 0; i < split.length - 1; i++) {
            for (int j = i+1; j < split.length; j++) {
                String str1 = split[i] + "-" + split[j];
                context.write(new Text(str1), new Text(str));
            }
        }
    }
}
