package home;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class home {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "home");
        job.setJarByClass(home.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("/home/in"));

        job.setMapperClass(M.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(homeBean.class);

        job.setReducerClass(R.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/home/out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class M extends Mapper<LongWritable, Text, Text, homeBean> {
    //省份0 城市1 城区2 小区名称3 户主姓名4 设备id5 设备类型6
    //房屋1采集值7 房屋2采集值8 房屋3采集值9 采集时间10
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(",");

        String province = line[0];
        String city = line[1];
        String area = line[2];
        String village = line[3];
        String name = line[4];
        String id = line[5];
        String type = line[6];
        String value1 = line[7];
        String value2 = line[8];
        String value3 = line[9];
        String time = line[10];

        homeBean bean = new homeBean();
        bean.setProvince(province);
        bean.setCity(city);
        bean.setArea(area);
        bean.setVillage(village);
        bean.setName(name);
        bean.setId(id);
        bean.setType(type);
        bean.setValue1(value1);
        bean.setValue2(value2);
        bean.setValue3(value3);
        bean.setTime(time);

        context.write(new Text(name), bean);
    }
}

class R extends Reducer<Text, homeBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<homeBean> values, Context context) throws IOException, InterruptedException {
        for (homeBean i : values) {
            if (!i.getArea().contains("区")) {
                i.setArea(i.getArea() + "区");
            }
            if (i.getType().equals("温度") || i.getType().equals("甲醛")) {
                if (i.getValue1().contains("%")) {
                    i.setValue1(i.getValue1() + "%");
                }
                if (i.getValue2().contains("%")) {
                    i.setValue2(i.getValue2() + "%");
                }
                if (i.getValue3().contains("%")) {
                    i.setValue3(i.getValue3() + "%");
                }
            }
            if (!(i.getValue1().equals("none") ||
                    i.getValue2().equals("none") ||
                    i.getValue3().equals("none"))) {
                context.write(new Text(i.toString()),new Text(""));
            }
        }
    }
}

class homeBean implements Writable {
    private String province;
    private String city;
    private String area;
    private String village;
    private String name;
    private String id;
    private String type;
    private String value1;
    private String value2;
    private String value3;
    private String time;

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getVillage() {
        return village;
    }

    public void setVillage(String village) {
        this.village = village;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(String value1) {
        this.value1 = value1;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
    }

    public String getValue3() {
        return value3;
    }

    public void setValue3(String value3) {
        this.value3 = value3;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return province + ' ' + city + ' ' + area + ' ' +
                village + ' ' + name + ' ' + id + ' ' +
                type + ' ' + value1 + ' ' + value2 + ' ' +
                value3 + ' ' + time;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(province);
        dataOutput.writeUTF(city);
        dataOutput.writeUTF(village);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(area);
        dataOutput.writeUTF(value1);
        dataOutput.writeUTF(value2);
        dataOutput.writeUTF(value3);
        dataOutput.writeUTF(time);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.province = dataInput.readUTF();
        this.city = dataInput.readUTF();
        this.village = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.id = dataInput.readUTF();
        this.area = dataInput.readUTF();
        this.value1 = dataInput.readUTF();
        this.value2 = dataInput.readUTF();
        this.value3 = dataInput.readUTF();
        this.time = dataInput.readUTF();
    }
}
