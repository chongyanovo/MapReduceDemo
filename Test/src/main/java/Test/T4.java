package Test;

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

public class T4 {
    private static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test11/output2/part-r-00000");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test11/T4/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(T4.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job,INPUT_PATH);

        job.setMapperClass(M4.class);
        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R4.class);
        job.setOutputKeyClass(Bean.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M4 extends Mapper<LongWritable, Text, Bean, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        Bean bean = new Bean();
        bean.setName(line[7]);
        String time = line[5].trim().substring(0, 4);
        bean.setTime(time);
        context.write(bean, new IntWritable(1));
    }
}

class R4 extends Reducer<Bean, IntWritable, Bean, IntWritable> {
    @Override
    protected void reduce(Bean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}