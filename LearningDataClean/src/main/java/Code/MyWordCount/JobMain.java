package MyWordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地window
        Job job = Job.getInstance(conf, "MyWordCount");
        job.setJarByClass(JobMain.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //FileInputFormat.setInputPaths(job, new Path("/in"));
        FileInputFormat.setInputPaths(job, new Path("H:\\mapreduce\\in"));
        //FileOutputFormat.setOutputPath(job, new Path("/out"));
        FileOutputFormat.setOutputPath(job, new Path("H:\\mapreduce\\out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
