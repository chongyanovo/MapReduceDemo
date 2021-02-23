package Test.Test5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/*
任务1、将不同分院的学生信息导出到不同的目录下（目录名称为分院的名称），每个目录只保存本目录的班级信息
输出结果举例：
目录名称：人工智能
目录数据：
人工智能   软件1901   24      33
人工智能   软件1902   20      30
目录名称：智能交通
目录数据：
智能交通   新能源2001    30     20
智能交通   新能源2002    30     15
目录名称：数字商务
目录数据：
数字商务   物流1901      15    15
数字商务   电子商务1901   5    30
 */
//分区 分院名称  班级名称  男生数量  女生数量
public class Q1 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test5/Q1/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/DataClean/src/test/mapreduce/Test/Test5/Q1/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q1");
        job.setJarByClass(Q1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(MyMap1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mykey1.class);

        job.setPartitionerClass(MyPartition1.class);

        job.setReducerClass(MyReduce1.class);
        job.setOutputKeyClass(Mykey1.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(3);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class MyMap1 extends Mapper<LongWritable, Text, Text, Mykey1> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");
        Mykey1 mykey = new Mykey1();
        mykey.setCollage(line[0]);
        mykey.setClazz(line[1]);
        mykey.setBoy(Integer.parseInt(line[2]));
        mykey.setGirl(Integer.parseInt(line[3]));
        context.write(new Text(line[0]), mykey);
    }
}

class MyPartition1 extends Partitioner<Text, Mykey1> {

    @Override
    public int getPartition(Text text, Mykey1 mykey, int i) {
        if (text.equals("人工智能")) {
            return 0;
        } else if (text.equals("智能交通")) {
            return 1;
        } else {
            return 2;
        }
    }
}

class MyReduce1 extends Reducer<Text, Mykey1, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Mykey1> values, Context context) throws IOException, InterruptedException {
        String str = "目录名称: " + key.toString();
        context.write(new Text(str), NullWritable.get());
        context.write(new Text("目录数据: "), NullWritable.get());
        for (Mykey1 i : values) {
            context.write(new Text(i.toString()), NullWritable.get());
        }
    }
}


class Mykey1 implements Writable {
    private String collage;
    private String clazz;
    private int boy;
    private int girl;

    public String getCollage() {
        return collage;
    }

    public void setCollage(String collage) {
        this.collage = collage;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public int getBoy() {
        return boy;
    }

    public void setBoy(int boy) {
        this.boy = boy;
    }

    public int getGirl() {
        return girl;
    }

    public void setGirl(int girl) {
        this.girl = girl;
    }

    @Override
    public String toString() {
        return collage + '\t' + clazz + '\t' + boy + '\t' + girl;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(collage);
        dataOutput.writeUTF(clazz);
        dataOutput.writeInt(boy);
        dataOutput.writeInt(girl);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.collage = dataInput.readUTF();
        this.clazz = dataInput.readUTF();
        this.boy = dataInput.readInt();
        this.girl = dataInput.readInt();
    }
}