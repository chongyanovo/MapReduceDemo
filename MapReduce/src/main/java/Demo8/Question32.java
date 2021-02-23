package Demo8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author mars
 * 任务3 将图书表、用户信息表、评价表合并为总表，并将总表放到目录total下
 * 商品名称           用户名称    评级   评论内容   评论日期             出版社           销售量
 * c++程序设计基础     我好怕怕       3      有些错别字   2020-8-20    电子工业出版社     4
 * java程序设计     你好怕怕       3级     内容太旧了   2021-9-2       电子工业出版社     3
 * 大数据技术基础     王春芳     4级     内容不错     2021-2-4         清华大学出版社      2
 * ……
 */
public class Question32 {
//    private static Path MapINPUT_PATH = new Path("hdfs://localhost:9000/Demo8/total/step1");
//    private static Path UserINPUT_PATH = new Path("hdfs://localhost:9000/Demo8/output/user2");
//    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Demo8/total/step2");

    private static Path MapINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/total/step1");
    private static Path UserINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/out1/user2");
    private static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/total/step2");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Question32.class);

        MultipleInputs.addInputPath(job, MapINPUT_PATH, TextInputFormat.class, Map.class);
        MultipleInputs.addInputPath(job, UserINPUT_PATH, TextInputFormat.class, UserMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueBean.class);

        job.setReducerClass(R32.class);
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

class Map extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        int i = line.length;
        if (line.length == 7) {
            String str = line[0] + "," + line[2] + "," + line[3] + "," + line[4] + "," + line[5] + "," + line[6];
            ValueBean valueBean = new ValueBean();
            valueBean.setValue(str);
            valueBean.setFlag("first");
            context.write(new Text(line[1]), valueBean);
        }
    }
}

class UserMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(line[1]);
        valueBean.setFlag("second");
        context.write(new Text(line[0]), valueBean);
    }
}

class R32 extends Reducer<Text, ValueBean, Text, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> list = new ArrayList<>();
        String first = "";
        String second = "";
        for (ValueBean i : values) {
            if (i.getFlag().equals("second")) {
                second = i.toString();
            } else {
                first = i.toString();
                list.add(first);
            }
        }
        for (String j : list) {
            String[] split = first.split(",");
            String split1 = split[0];
            String split2 = split[1] + "," + split[2] + "," + split[3] + "," + split[4] + "," + split[5];
            String str = split1 + "," + second + "," + split2;
            context.write(new Text(str), NullWritable.get());
        }
    }
}
