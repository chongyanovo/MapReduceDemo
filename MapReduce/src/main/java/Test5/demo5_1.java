package Test5;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
public class demo5_1 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo5/demo5_1/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo5/demo5_1/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(demo5_1.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Map_demo5_1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyKey.class);

        job.setReducerClass(MultiOutPutReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


class Map_demo5_1 extends Mapper<LongWritable, Text, Text, MyKey> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        MyKey myKey = new MyKey();
        myKey.setCollage(line[0]);
        myKey.setClazz(line[1]);
        myKey.setBoy(Integer.parseInt(line[2]));
        myKey.setGirl(Integer.parseInt(line[3]));
        context.write(new Text(line[0]), myKey);
    }
}

//输出到多个文件或多个文件夹，驱动中不需要任何改变，只需要加个multipleOutputs的变量和setup()、cleanup()函数
//然后就可以用mos.write(Key key,Value value,String baseOutputPath)代替context.write(key, value);
class MultiOutPutReducer extends Reducer<Text, MyKey, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<MyKey> values, Context context) throws IOException, InterruptedException {

        for (MyKey i : values) {
            mos.write(NullWritable.get(), new Text(i.toString()), key.toString());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (null != mos) {
            mos.close();
            mos = null;
        }
    }

}

