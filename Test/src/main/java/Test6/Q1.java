package Test6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

/**
 * @author mars
 * 任务1、统计每个url的访问数量
 * 输出结果举例：
 * POST  /service/1.htm  3  【本条数据表示：发送post请求访问/service/1.htm的次数为3（一个url可能会发送post请求，也可能会发送get请求）】
 * GET   /service/1.htm  2  【本条数据表示：发送get请求访问/service/1.htm的次数为2，（一个url可能会发送post请求，也可能会发送get请求）】
 * GET   /    4
 */
public class Q1 {
    public static Path INPUT_PATH = new Path("hdfs://localhost:9000/Test/input");
    public static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Test/output/output1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Q1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(M1.class);
        job.setMapOutputKeyClass(Key1.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(R1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M1 extends Mapper<LongWritable, Text, Key1, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String post = "POST";
        String get = "GET";
        String http = "HTTP/1.0";
        Key1 key1 = new Key1();
        int i;
        int j = line.indexOf(http);
        if (line.contains(post)) {
            i = line.lastIndexOf(post);
            key1.setType(post);
        } else {
            i = line.lastIndexOf(get);
            key1.setType(get);
        }
        String url = line.substring(i, j).trim();
        key1.setUrl(url);
        context.write(key1, new IntWritable(1));
    }
}

class R1 extends Reducer<Key1, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Key1 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(new Text(key.toString()), new IntWritable(sum));
    }
}

class Key1 implements WritableComparable<Key1> {
    private String type;
    private String url;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return type + "\t" + url;
    }

    @Override
    public int compareTo(Key1 o) {
        int result = this.type.compareTo(o.getType());
        if (result == 0) {
            result = this.url.compareTo(o.getUrl());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(url);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.url = dataInput.readUTF();
    }
}