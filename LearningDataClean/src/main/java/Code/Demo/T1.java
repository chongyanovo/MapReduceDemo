package Demo;

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

public class T1 {
    //1、计算人工智能学院的学生总数是多少？
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
//        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, "T1");
        job.setJarByClass(T1.class);

        job.setInputFormatClass(TextInputFormat.class);
        //FileInputFormat.setInputPaths(job, new Path("H:\\测试\\in"));
        FileInputFormat.setInputPaths(job, new Path("/in"));

        job.setMapperClass(M1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        //FileOutputFormat.setOutputPath(job, new Path("H:\\测试\\out1"));
        FileOutputFormat.setOutputPath(job, new Path("/out1"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    //0.分院 1.专业 2.班级 3.课程名称 4.班级人数 5.平均成绩
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] arr = value.toString().split(",");
        String college = arr[0];
        context.write(new Text(college), new IntWritable(1));
    }
}

class R1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    Integer count = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable i : values) {
            count++;
        }
        context.write(key, new IntWritable(count));
    }
}
