package Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class T3 {
    private static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test11/output2/part-r-00000");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test11/T3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(T3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job,INPUT_PATH);

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R3.class);
        job.setOutputKeyClass(Bean.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M3 extends Mapper<LongWritable, Text, Bean, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        Bean bean = new Bean();
        bean.setName(line[10]);
        String time = line[5].trim().substring(0, 7);
        bean.setTime(time);
        int num = Integer.parseInt(line[3]);
        context.write(bean, new IntWritable(num));
    }
}

class R3 extends Reducer<Bean, IntWritable, Bean, IntWritable> {
    @Override
    protected void reduce(Bean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}