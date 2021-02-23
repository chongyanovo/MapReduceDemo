package Code.wendusort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;//这个不能选错其他类型的text
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        Configuration conf = new Configuration() ;
        conf.set("fs.defaultFS","hdfs://master:9000");
        FileSystem fs =null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Job job = null;
        try {
            job = Job.getInstance(conf,"wendusort");
        } catch (IOException e) {
            e.printStackTrace();
        }

        job.setJarByClass(Main.class);
        job.setMapperClass(MyMapper.class);
        //以key value方式解析数据，然后交给mapper处理,分隔key和value的分隔符默认是\t
        //job.setInputFormatClass(KeyValueTextInputFormat.class);

        //，所产生的主键Key就是当前行在整个文本文件中的字符偏移量，而value就是该行的内容。
        job.setInputFormatClass(TextInputFormat.class);

        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroup.class);
        job.setNumReduceTasks(2);//生成2个part-r-00000文件

        job.setOutputKeyClass(MyKey.class);
        job.setOutputValueClass(Text.class);

        try {
            FileInputFormat.addInputPath(job,new Path("/in22"));
            Path outpath = new Path("/out22");
            if (fs.exists(outpath)){
                fs.delete(outpath,true);
            }
            FileOutputFormat.setOutputPath(job,outpath);
            boolean f = job.waitForCompletion(true);
            System.out.println("f:" + f);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
