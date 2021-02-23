package Test;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class T2 {
    private static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test11/output2/part-r-00000");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test11/T2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(T2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job,INPUT_PATH);

        job.setMapperClass(M2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M2 extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        //6,1,2,3,5
        String str = line[6] + "\t" + line[1] + "\t" + line[2] + "\t" + line[3] + "\t" + line[5];
        context.write(new Text(str), NullWritable.get());
    }
}