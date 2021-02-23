package WenduSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class MyMapper extends Mapper<LongWritable, Text, Bean, Text> {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Counter In = context.getCounter("WenduSort_Mapper", "InNum");
        In.increment(1L);
        String[] line = value.toString().split(",");
        if (line.length == 2) {
            {
                Counter Out = context.getCounter("WenduSort_Mapper", "OutNum");
                Out.increment(1L);
            }
        }
//        String[] split = line[0].split("-");

//        Bean bean = new Bean();
//        bean.setYear(Integer.parseInt(split[0]));
//        bean.setWendu(Integer.parseInt(line[1].substring(0, line[1].indexOf("°C"))));
//        context.write(bean, value);

        if (line.length == 2) {
            System.out.println(line.length);
            System.out.println(line[0]);
            System.out.println(line[1]);
        try {
            Date date = sdf.parse(line[0]);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int year = c.get(1);
            //String wendu = line[1].substring(0, line[1].indexOf("°C"));
            int wendu = Integer.parseInt(line[1].substring(0, line[1].indexOf("°C")));
            Bean bean = new Bean();
            //bean.setYear(String.valueOf(year));
            bean.setYear(year);
            bean.setWendu(wendu);
            context.write(bean, value);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        }

    }
}
