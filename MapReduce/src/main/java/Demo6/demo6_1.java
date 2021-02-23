package Demo6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

 /*
任务1、统计每个url的访问数量
输出结果举例：
POST  /service/1.htm  3  【本条数据表示：发送post请求访问/service/1.htm的次数为3（一个url可能会发送post请求，也可能会发送get请求）】
GET   /service/1.htm  2  【本条数据表示：发送get请求访问/service/1.htm的次数为2，（一个url可能会发送post请求，也可能会发送get请求）】
GET   /    4
 */
/**
访问ip 访问时间 请求类型 url 协议名称 http返回码 响应时长
 */
/**
 * @author mars
 */
public class demo6_1 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_1/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo6/demo6_1/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "demo6_1");
        job.setJarByClass(demo6_1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Map_demo6_1.class);
        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Reduce_demo6_1.class);
        job.setOutputKeyClass(MyKey.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Map_demo6_1 extends Mapper<LongWritable, Text, MyKey, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        int i = "POST".length();
        String type;
        if (str.contains("POST")) {
            i += str.indexOf("POST");
            type = "POST";
        } else {
            i += str.indexOf("GET");
            type = "GET";
        }

        int j = str.indexOf("HTTP/1.0");
        String substring = str.substring(i, j);

        MyKey myKey = new MyKey();
        myKey.setType(type);
        myKey.setUrl(substring.trim());
        context.write(myKey, new IntWritable(1));
    }
}

class Reduce_demo6_1 extends Reducer<MyKey, IntWritable, MyKey, IntWritable> {
    @Override
    protected void reduce(MyKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}