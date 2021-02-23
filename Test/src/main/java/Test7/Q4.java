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

public class Q4 {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test7/Q1/output2");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test7/Q4/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Q4.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M4.class);
        job.setMapOutputKeyClass(Bean4.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R4.class);
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

class M4 extends Mapper<LongWritable, Text, Bean4, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split("\t");
        String year = line[5].trim().substring(0, 4);
        Bean4 bean = new Bean4();
        bean.setCompare(line[7]);
        bean.setYear(year);
        context.write(bean, new IntWritable(1));
    }
}

class R4 extends Reducer<Bean4, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Bean4 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(new Text(key.toString()), new IntWritable(sum));
    }
}

class Bean4 implements WritableComparable<Bean4> {
    private String compare;
    private String year;

    public String getCompare() {
        return compare;
    }

    public void setCompare(String compare) {
        this.compare = compare;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public String toString() {
        return compare + "\t" + year;
    }

    @Override
    public int compareTo(Bean4 bean) {
        int result = this.compare.compareTo(bean.getCompare());
        if (result == 0) {
            result = this.year.compareTo(bean.getYear());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(compare);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.compare = dataInput.readUTF();
        this.year = dataInput.readUTF();
    }
}