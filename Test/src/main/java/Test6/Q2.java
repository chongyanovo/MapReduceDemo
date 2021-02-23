package Test6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 * 任务2、将每一天访问的url放到不同的目录下（目录名称为日期）
 * 输出结果举例：
 * 目录名称：2013-03-04
 * 数据内容：
 * 172.18.35.1 - - [2013-03-04 23:38:27] "POST    /service/1.htm   HTTP/1.0"    200         1
 * 172.18.35.2 - - [2013-03-04 23:38:27] "GET service/1.htm HTTP/1.0" 200  1
 * 目录名称：2013-03-08
 * 数据内容：
 * 172.18.35.6 - - [2013-03-08 23:38:27] "POST / service/3.htm HTTP/1.0" 200 3
 * 172.18.36.1 - - [2013-03-08 23:38:27] "GET / service/1.htm HTTP/1.0" 200 4
 * 172.18.35.8 - - [2013-03-08 23:38:27] "GET /html/notes/1.html HTTP/1.0" 200 5
 */
public class Q2 {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test/input");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test/output/output2");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Q2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M2 extends Mapper<LongWritable, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        int i = line.indexOf("[");
        int j = line.indexOf("]");
        String time = line.substring(i + 1, j);
        String[] split = time.split(" ");
        String date = split[0];
        mos.write(value, NullWritable.get(), date + "/");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        mos = null;
    }
}
