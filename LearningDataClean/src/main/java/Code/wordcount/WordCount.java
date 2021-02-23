package Code.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * 词频统计
 */
public class WordCount
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
//        if(args.length != 2 || args == null)
//        {
//            System.out.println("please input Path!");
//            System.exit(0);
//        }
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        configuration.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(configuration, WordCount.class.getSimpleName());
        // 打jar包
        job.setJarByClass(WordCount.class);
        // 通过job设置输入/输出格式

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输入/输出路径
        //FileInputFormat.setInputPaths(job, new Path(args[0]));7
        FileInputFormat.setInputPaths(job, new Path("H:\\mapreduce\\in"));
//        FileInputFormat.setInputPaths(job, new Path("/in"));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("H:\\mapreduce\\out"));
//        FileOutputFormat.setOutputPath(job, new Path("/out"));
        // 设置处理Map/Reduce阶段的类
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);
        // 设置最终输出key/value的类型m
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 提交作业
        job.waitForCompletion(true);
    }
}

class WordMap extends Mapper<Object, Text, Text, IntWritable>
{
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String[] lines = value.toString().split(",");
        for (String word : lines)
        {
            // 每个单词出现１次，作为中间结果输出
            context.write(new Text(word), new IntWritable(1));
        }
    }
}

class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (IntWritable count : values)
        {
            sum = sum + count.get();
            //context.write(key, new IntWritable(sum));// 输出最终结果,放到这里会导致重复输出
        }
        context.write(key, new IntWritable(sum));// 输出最终结果
    }
}