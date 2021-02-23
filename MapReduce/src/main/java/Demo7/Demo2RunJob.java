package Demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
 * 任务2、根据汇总表统计客户的购物信息
 * 客户名   商品类型   商品名称      商品数量   订单时间
 * 张三      电脑      联想笔记本A      1       2020-09-23
 * 李四      办公       铅笔A           1       2020-09-24
 * 李四      办公       尺子B           2       2020-10-04
 */
public class Demo2RunJob {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_2/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "demo7_2");
        job.setJarByClass(Demo2RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Demo2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyKey2.class);

        job.setReducerClass(Demo2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Demo2Mapper extends Mapper<LongWritable, Text, Text, MyKey2> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String name = line[6];
        MyKey2 mykey = new MyKey2();
        mykey.setType(line[1]);
        mykey.setProductName(line[2]);
        mykey.setProductNum(line[3]);
        mykey.setTime(line[5]);
        context.write(new Text(name), mykey);
    }
}

class Demo2Reducer extends Reducer<Text, MyKey2, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<MyKey2> values, Context context) throws IOException, InterruptedException {
        for (MyKey2 i : values) {
            String str = key.toString() + "\t" + i.toString();
            context.write(new Text(str), NullWritable.get());
        }
    }
}

class MyKey2 implements Writable {
    private String type;
    private String productName;
    private String productNum;
    private String time;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductNum() {
        return productNum;
    }

    public void setProductNum(String productNum) {
        this.productNum = productNum;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return type + '\t' + productName + '\t' + productNum + '\t' + time;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(productName);
        dataOutput.writeUTF(productNum);
        dataOutput.writeUTF(time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.productName = dataInput.readUTF();
        this.productNum = dataInput.readUTF();
        this.time = dataInput.readUTF();
    }
}
