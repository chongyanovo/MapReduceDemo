package Test7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Q3 {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test7/Q1/output2");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test7/Q3/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Q3.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M3.class);
        job.setMapOutputKeyClass(Bean3.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R3.class);
        job.setOutputKeyClass(Text.class);
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

class M3 extends Mapper<LongWritable, Text, Bean3, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        String mouth = line[5].trim().substring(0, 7);
        Bean3 bean = new Bean3();
        bean.setCompary(line[10]);
        bean.setMouth(mouth);
        context.write(bean, new IntWritable(1));
    }
}

class R3 extends Reducer<Bean3, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Bean3 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(new Text(key.toString()), new IntWritable(sum));
    }
}

class Bean3 implements WritableComparable<Bean3> {
    private String compary;
    private String mouth;

    public String getCompary() {
        return compary;
    }

    public void setCompary(String compary) {
        this.compary = compary;
    }

    public String getMouth() {
        return mouth;
    }

    public void setMouth(String mouth) {
        this.mouth = mouth;
    }

    @Override
    public String toString() {
        return compary + "\t" + mouth;
    }

    @Override
    public int compareTo(Bean3 bean) {
        int result = this.compary.compareTo(bean.getCompary());
        if (result == 0) {
            result = this.mouth.compareTo(bean.getMouth());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(compary);
        dataOutput.writeUTF(mouth);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.compary = dataInput.readUTF();
        this.mouth = dataInput.readUTF();
    }
}
