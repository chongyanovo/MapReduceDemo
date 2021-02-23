package dianxin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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

/**
 * 电信数据统计：统计同一个用户的上行总流量和，下行总流量和以及上下总流量和
 * dianxin.txt的数据格式如下：
 * 手机号       上行流量  下行流量
 * 13560439658    20       50
 */
public class Dianxin1
{
    //public static String path1 = "/dianxin.txt";
    public static String path1 = "H:\\FlowCount\\in";
    //public static String path2 = "hdfs://master:9000/dianout/";
    public static String path2 = "H:\\FlowCount\\out";
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://master:9000/");
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(new Path(path2)))
        {
            fileSystem.delete(new Path(path2), true);
        }
        Job job = Job.getInstance(conf, Dianxin1.class.getSimpleName());
        job.setJarByClass(Dianxin1.class);

        //编写驱动
        FileInputFormat.setInputPaths(job, new Path(path1));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //shuffle洗牌阶段
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(path2));
        //将任务提交给JobTracker
        job.waitForCompletion(true);
        //查看程序的运行结果
//        FSDataInputStream fr = fileSystem.open(new Path("hdfs://master:9000/dianut/part-r-00000"));
//        IOUtils.copyBytes(fr,System.out,1024,true);
    }
}

class MyMapper   extends Mapper<LongWritable, Text, Text, Text>
{
    @Override
    protected void map(LongWritable k1, Text v1,Context context)throws IOException, InterruptedException
    {
        String line = v1.toString();//拿到日志中的一行数据
        String[] splited = line.split(" ");//切分各个字段
        //获取我们所需要的字段
        String msisdn = splited[0];
        String upFlow = splited[1];
        String downFlow = splited[2];
        long flowsum = Long.parseLong(upFlow) + Long.parseLong(downFlow);
        context.write(new Text(msisdn), new Text(upFlow+" "+downFlow+" "+String.valueOf(flowsum)));
    }
}

class MyReducer  extends Reducer<Text, Text, Text, Text>
{
    @Override
    protected void reduce(Text k2, Iterable<Text> v2s,Context context)throws IOException, InterruptedException
    {
        long upFlowSum = 0L;
        long downFlowSum = 0L;
        long FlowSum = 0L;
        for(Text v2:v2s)
        {
            String[] splited = v2.toString().split(" ");
            upFlowSum += Long.parseLong(splited[0]);
            downFlowSum += Long.parseLong(splited[1]);
            FlowSum += Long.parseLong(splited[2]);
        }
        String data = String.valueOf(upFlowSum)+" "+String.valueOf(downFlowSum)+" "+String.valueOf(FlowSum);
        context.write(k2,new Text(data));
    }
}