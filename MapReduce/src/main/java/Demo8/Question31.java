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

/**
 * @author mars
 * 任务3 将图书表、用户信息表、评价表合并为总表，并将总表放到目录total下
 * 商品名称           用户名称    评级   评论内容   评论日期             出版社           销售量
 * c++程序设计基础     我好怕怕       3      有些错别字   2020-8-20    电子工业出版社     4
 * java程序设计     你好怕怕       3级     内容太旧了   2021-9-2       电子工业出版社     3
 * 大数据技术基础     王春芳     4级     内容不错     2021-2-4         清华大学出版社      2
 * ……
 */
public class Question31 {
    private static Path BookINPUT_PATH = new Path("hdfs://localhost:9000/Demo8/book/book.csv");
    private static Path CommentINPUT_PATH = new Path("hdfs://localhost:9000/Demo8/output/comment3");
    private static Path OUTPUT_PATH = new Path("hdfs://localhost:9000/Demo8/total/step1");

//    private static Path BookINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/book.csv");
//    private static Path CommentINPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/comment3");
//    private static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo8/total/step1");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Question31.class);

        MultipleInputs.addInputPath(job, CommentINPUT_PATH, TextInputFormat.class, CommentMap.class);
        MultipleInputs.addInputPath(job, BookINPUT_PATH, TextInputFormat.class, BookMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ValueBean.class);

        job.setReducerClass(R31.class);
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

class BookMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String str = line[1] + "," + line[2] + "," + line[3];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(str);
        valueBean.setFlag("first");
        context.write(new Text(line[0]), valueBean);
    }
}

class CommentMap extends Mapper<LongWritable, Text, Text, ValueBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String str = line[1] + "," + line[2] + "," + line[3] + "," + line[4];
        ValueBean valueBean = new ValueBean();
        valueBean.setValue(str);
        valueBean.setFlag("second");
        context.write(new Text(line[0]), valueBean);
    }
}

class R31 extends Reducer<Text, ValueBean, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ValueBean> values, Context context) throws IOException, InterruptedException {
        String first = "";
        String second = "";
        for (ValueBean i : values) {
            if (i.getFlag().equals("first")) {
                first = i.toString();
            } else {
                second = i.toString();
            }
        }
        String[] split = first.split(",");
        String split1 = split[0];
        String split2 = split[1] + "," + split[2];
        String str = split1 + "," + second + "," + split2;
        context.write(new Text(str), NullWritable.get());
    }
}
