import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Test4 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "MeteorologicalDataCount");
        job.setJarByClass(Test4.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("H:\\考试\\in"));

        job.setMapperClass(Mapper4.class);
        job.setMapOutputKeyClass(Text.class);//k2
        job.setMapOutputValueClass(NullWritable.class);//v2

        job.setReducerClass(Reducer4.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("H:\\考试\\out4"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Mapper4 extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //context.getCounter("SumMapper","Sum").increment(1L);
        context.getCounter("Sum","Sum").increment(1L);
        String replaceAll = value.toString().replaceAll(" ", "\t");
        context.write(new Text(replaceAll),NullWritable.get());
    }
}
class Reducer4 extends Reducer<Text, NullWritable,Text, NullWritable>{
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //context.getCounter("SumReducer","Sum").increment(1L);
        context.write(key,NullWritable.get());
    }
}
