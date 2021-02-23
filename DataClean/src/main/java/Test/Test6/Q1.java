package Test.Test6;

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

/*
任务1、统计每个url的访问数量
输出结果举例：
POST  /service/1.htm  3  【本条数据表示：发送post请求访问/service/1.htm的次数为3（一个url可能会发送post请求，也可能会发送get请求）】
GET   /service/1.htm  2  【本条数据表示：发送get请求访问/service/1.htm的次数为2，（一个url可能会发送post请求，也可能会发送get请求）】
GET   /    4
 */
//访问ip 访问时间 请求类型 url 协议名称 http返回码 响应时长
public class Q1 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test6/Q1/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test6/Q1/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q1");
        job.setJarByClass(Q1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(MyMap1.class);
        job.setMapOutputKeyClass(MyKey1.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReduce1.class);
        job.setOutputKeyClass(MyKey1.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class MyMap1 extends Mapper<LongWritable, Text, MyKey1, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        int i;
        String type;
        if (str.contains("POST")) {
            i = str.indexOf("POST");
            type = "POST";
        } else {
            i = str.indexOf("GET");
            type = "GET";
        }

        int j = str.indexOf("HTTP/1.0");
        String substring = str.substring(i, j);
        String replace = substring.replace(" ", "");

        MyKey1 myKey = new MyKey1();
        myKey.setType(type);
        myKey.setUrl(replace);
        context.write(myKey, new IntWritable(1));
    }
}

class MyReduce1 extends Reducer<MyKey1, IntWritable, MyKey1, IntWritable> {
    @Override
    protected void reduce(MyKey1 key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values) {
            sum += i.get();
        }
        context.write(key, new IntWritable(sum));
    }
}

class MyKey1 implements WritableComparable<MyKey1> {
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
        return type + "\t" + url + "\t";
    }

    @Override
    public int compareTo(MyKey1 myKey) {
        int result = myKey.getType().compareTo(this.type);
        if (result == 0) {
            result = myKey.getUrl().compareTo(this.url);
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