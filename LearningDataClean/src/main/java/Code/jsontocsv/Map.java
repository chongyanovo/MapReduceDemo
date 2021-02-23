package jsontocsv;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<Object, Text,Text, NullWritable>
{
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //本方法把json数据提取出来，以文本的方式合成为一行一个记录
        String Line=value.toString();
        Text txt=new Text();
        NullWritable n=NullWritable.get();
        try
        {
            context.getCounter("count", "dzy_count").increment(1);
            JSONObject json=JSONObject.parseObject(Line);
            String city = json.getString("city");
            String location = json.getString("location");
            String model = json.getString("model");
            String size = json.getString("size");
            String equipment = json.getString("equipment");
            String money = json.getString("money");
            String time = json.getString("time");
            String s=city+","+location+","+model+","+size+","+equipment+","+money+","+time;
            txt.set(s);
            context.write(txt,n);
        }
        catch (Exception e)
        {

        }
    }
}