package jsontocsv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.channels.FileLockInterruptionException;

/**
 * 将json格式的数据转换为csv格式
 */
public class ex {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("mapreduce.framework.name", "local"); //设置mapreduce框架为本地
//        conf.set("fs.defaultF", "file:///"); //设置文件系统为本地windows
        Job job = Job.getInstance(conf);
        job.setJarByClass(ex.class);
        job.setMapperClass(Map.class);
        //job.setReducerClass(Red.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        Path in=new Path("/in");
        //Path in = new Path("H:\\mapreduceTest\\in");
        Path out=new Path("/out");
        //Path out = new Path("H:\\mapreduceTest\\out");

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.waitForCompletion(true);
    }
}

