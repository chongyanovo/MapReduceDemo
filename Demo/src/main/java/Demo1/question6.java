package Demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class question6 {
    private static Path INPATH = new Path("hdfs://localhost:9000/demo1/in");
    private static Path OUTPATH = new Path("hdfs://localhost:9000/demo1/out6");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(question6.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPATH);

        job.setMapperClass(mapper6.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

//6 将不同城市的销售数据存放到不同文件夹中，每个文件夹按城市名称命名。
class mapper6 extends Mapper<LongWritable, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().trim().split("\t");
        String city = lines[2];
        mos.write(value, NullWritable.get(), city + "/");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        mos = null;
    }
}


