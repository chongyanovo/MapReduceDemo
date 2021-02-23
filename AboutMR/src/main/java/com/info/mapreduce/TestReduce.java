package com.info.mapreduce;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import com.info.util.*;

/**
 * 测试实现类 reduce
 *
 * @author panda
 */
public class TestReduce extends Reducer<Text, LongWritable, Text, Text> {
    Logger log = Logger.getLogger(this.getClass());

    @Override
    protected void reduce(Text key, List<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        for (Object obj : values) {
            sum += (Long.parseLong((String) obj));
        }
        context.write(key, new Text(sum + ""));
    }
}