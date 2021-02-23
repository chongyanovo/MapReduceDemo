package Demo7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.time.temporal.Temporal;

/**
 * @author mars
 * 任务6、根据汇总表将不同商品类型的订单放到不同的目录下
 * 目录名称：电脑
 * 数据：
 * 商品编号 商品类型 商品名称     商品数量 订单id   订单时间  客户名 快递公司   客户住址     客户所在省份   供应商名称
 * A113      电脑    联想笔记本A     1      B1123   2020-09-23  张三   中通       仙林大学城    江苏         A公司
 * 目录名称：办公
 * 数据：
 * 商品编号 商品类型 商品名称     商品数量 订单id   订单时间  客户名 快递公司   客户住址     客户所在省份   供应商名称
 * A114      办公    铅笔A           1      B1124   2020-09-24  李四   申通       江宁          江苏         B公司
 * A115      办公    尺子B           2      B1125   2020-10-04  李四   申通       江宁          江苏         B公司
 * A116      办公    橡皮A           3      B1126   2020-11-5   李冰   顺丰       杭州          浙江         B公司
 */
public class Demo6RunJob {
    final static Path INPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_6/input");
    final static Path OUTPUT_PATH = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/demo7/demo7_6/output");


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Demo6");
        job.setJarByClass(Demo6RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH);

        job.setMapperClass(Demo6Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //MultipleOutputs.addNamedOutput(job,"Out",TextOutputFormat.class,NullWritable.class,Text.class);

        FileSystem fileSystem = OUTPUT_PATH.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH)) {
            fileSystem.delete(OUTPUT_PATH, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Demo6Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().trim().split(",");
        String type = line[1];
        mos.write(NullWritable.get(), value, type+"/");
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
        mos = null;
    }
}

