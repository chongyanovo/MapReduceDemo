package Code.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 天气数据统计
 */
public class Weather
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        if(args.length != 2 || args == null)
        {
            System.out.println("please input Path!");
            System.exit(0);
        }
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, Weather.class.getSimpleName());
        // 打jar包
        job.setJarByClass(Weather.class);
        // 通过job设置输入/输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置处理Map/Reduce阶段的类
        job.setMapperClass(DaysMap.class);
        job.setReducerClass(DaysReduce.class);
        // 设置最终输出key/value的类型m
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 提交作业
        job.waitForCompletion(true);
    }
}

class DaysMap extends Mapper<Object, Text, Text, IntWritable>
{
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] lines = value.toString().split("\n");
        for (String line : lines)
        {
//			System.out.println("line=  " + line);
            String[] words = line.toString().split("\t");
//			System.out.println("words[0]=  " + words[0]);
//			System.out.println("words[1]=  " + words[1]);

            // 每个单词出现１次，作为中间结果输出
            context.write(new Text(words[1]), new IntWritable(1));
        }
    }
}

class DaysReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : values) {
            sum = sum + count.get();
            //context.write(key, new IntWritable(sum));// 输出最终结果,放到这里会导致重复输出
        }
        context.write(key, new IntWritable(sum));
    }
}