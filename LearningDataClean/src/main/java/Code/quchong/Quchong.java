package WordDistinct;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 对文件中的字符串去重处理
 *
 */
public class Quchong
{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
//        if(args.length != 2 || args == null)
//        {
//            System.err.println("Please input Path!");
//            System.exit(0);
//        }
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf, Quchong.class.getSimpleName());
        job.setJarByClass(Quchong.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("H:\\quchong\\in"));
        FileOutputFormat.setOutputPath(job, new Path("H:\\quchong\\out"));

        job.setMapperClass(WordDistinctMap.class);
//		job.setCombinerClass(WordDistinctReduce.class);
        job.setReducerClass(WordDistinctReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}

class WordDistinctMap extends Mapper<Object, Text, Text, Text>
{
    protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException
    {
        context.write(value, new Text(""));
    }
}

class WordDistinctReduce extends Reducer<Text, Text, Text, Text> {
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, new Text(""));
    }
}