package Demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author mars
 * 任务1、将销售表、订单表和商品表合并为汇总表
 * 商品编号 商品类型 商品名称     商品数量 订单id   订单时间  客户名 快递公司   客户住址     客户所在省份   供应商名称
 * A113      电脑    联想笔记本A     1      B1123   2020-09-23  张三   中通       仙林大学城    江苏         A公司
 * A114      办公    铅笔A           1      B1124   2020-09-24  李四   申通       江宁          江苏         B公司
 * A115      办公    尺子B           2      B1125   2020-10-04  李四   申通       江宁          江苏         B公司
 * A116      办公    橡皮A           3      B1126   2020-11-5   李冰   顺丰       杭州          浙江         B公司
 */
public class Demo1RunJob2 {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_2/input");
    final static Path INPUT_PATH1 = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_2/input/part-r-00000");
    final static Path INPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_2/input/order.txt");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_1_2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "demo7_1_2");
        job.setJarByClass(Demo1RunJob2.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        MultipleInputs.addInputPath(job, INPUT_PATH1, TextInputFormat.class, MyMapper.class);
        MultipleInputs.addInputPath(job, INPUT_PATH2, TextInputFormat.class, OrderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Demo1Reducer2.class);
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

class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        context.write(new Text(line[5]), value);
    }
}

class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        context.write(new Text(line[0]), new Text(value));
    }
}

class Demo1Reducer2 extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String str = "";
        String first = "";
        String second = "";
        String company = "";
        for (Text i : values) {
            if (i.toString().startsWith("A")) {
                str = i.toString();
                String[] line = str.split(",");
                company = line[3];
                first = line[0] + "," + line[1] + "," + line[2] + "," + line[4] + ",";
            } else {
                second += i.toString();
            }
        }

        context.write(new Text(first + second + "," + company), NullWritable.get());
    }
}