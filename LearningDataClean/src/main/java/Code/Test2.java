import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Test2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "MeteorologicalDataCount");
        job.setJarByClass(Test2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("H:\\考试\\in"));

        job.setMapperClass(Mapper2.class);
        job.setMapOutputKeyClass(Text.class);//k2
        job.setMapOutputValueClass(Text.class);//v2

        job.setReducerClass(Reducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("H:\\考试\\out2"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String replaceAll = value.toString().replaceAll(" ", "\t");
        String[] split = replaceAll.split("\t");
        context.write(new Text(split[0]), new Text(split[4]));
    }
}

class Reducer2 extends Reducer<Text, Text, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Integer count = 0;
        Set<Text> set = new HashSet<Text>();
        for (Text i : values) {
            if (!set.contains(i)) {
                set.add(i);
                count++;
            }
        }
        context.write(key, new IntWritable(count));
    }
}