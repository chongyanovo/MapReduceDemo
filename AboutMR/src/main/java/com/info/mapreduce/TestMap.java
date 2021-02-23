package com.info.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import com.info.util.*;

/**
 * 测试实现类 mapper
 *
 * @author panda
 */

public class TestMap extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
//  LOG.info("map输入 key=" + key);

//  LOG.info("map输入 value=" + value);
        String v[] = value.toString().split(",");
        context.write(new Text(v[0]), new LongWritable(1));
    }

}