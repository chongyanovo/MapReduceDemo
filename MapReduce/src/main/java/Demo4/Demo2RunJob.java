package Demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeMap;

/**
 * @author mars
 * 任务2、分别找出男性和女性感染人群中年龄最大的患者姓名
 * 输出结果举例：
 * 黄石市	90	男	卢洪河
 * 武汉市	70	女	赵雅
 */
public class Demo2RunJob {
    public static Path INPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo4/demo_2/input");
    public static Path OUTPUT_PATH2 = new Path("/Volumes/software/IdeaProjects/MapReduce/src/test/java/Demo4/demo_2/output");

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "T22");
        job.setJarByClass(Demo2RunJob.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, INPUT_PATH2);

        job.setMapperClass(Demo2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Demo2reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fileSystem = OUTPUT_PATH2.getFileSystem(conf);
        if (fileSystem.exists(OUTPUT_PATH2)) {
            fileSystem.delete(OUTPUT_PATH2, true);
        }

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, OUTPUT_PATH2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class Demo2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        context.write(new Text(line[2]), value);
    }
}

class Demo2reducer extends Reducer<Text, Text, Text, NullWritable> {
    //    TreeMap<Integer, String> mapMan = new TreeMap<>();
//    TreeMap<Integer, String> mapWoMan = new TreeMap<>();
    private TreeMap<Integer, String> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text i : values) {
            String[] line = i.toString().split("\t");
            int age = Integer.parseInt(line[1]);
            String sex = line[2];
//            if (sex.equals("男")) {
//                mapMan.put(age, i.toString());
//            } else {
//                mapWoMan.put(age, i.toString());
//            }
            map.put(age, i.toString());
            if (map.size() > 1) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
//        ArrayList<String> listMan = new ArrayList();
//        for (Integer i : mapMan.keySet()) {
//            listMan.add(mapMan.get(i));
//        }
//        Collections.reverse(listMan);
//        context.write(new Text(listMan.get(0)), NullWritable.get());
//
//        ArrayList<String> listWoman = new ArrayList();
////        for (Integer j : mapWoMan.keySet()) {
////            listWoman.add(listWoman.get(j));
////        }
////        Collections.reverse(listWoman);
////        context.write(new Text(listWoman.get(0)), NullWritable.get());

//        ArrayList<String> list = new ArrayList();
//        for (Integer j : map.keySet()) {
//            list.add(map.get(j));
//        }
//        Collections.reverse(list);
//        context.write(new Text(list.get(0)), NullWritable.get());

        context.write(new Text(map.get(map.firstKey())), NullWritable.get());
    }
}

