package MyWordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<Text, LongWritable, Text, IntWritable> {
    @Override
    protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split(",");
        for (String i : str) {
            context.write(new Text(i),new IntWritable(1));
        }
    }
}
