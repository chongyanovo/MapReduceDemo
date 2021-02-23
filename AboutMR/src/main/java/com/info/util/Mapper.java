package com.info.util;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@SuppressWarnings("unchecked")
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public int COUNT = 0;
    public String mapperId = UUID.randomUUID().toString();
    public Logger LOG = Logger.getLogger(Mapper.class);

    /**
     * Called once at the beginning of the task.
     */
    protected void setup(Context context) throws IOException,
            InterruptedException {
// NOTHING
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * <p>
     * should override this, but the default is the identity function.
     */
    protected void map(KEYIN key, VALUEIN value, Context context)
            throws IOException, InterruptedException {
        context.write((KEYOUT) key, (VALUEOUT) value);
    }

    /**
     * Called once at the end of the task.
     */
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
// NOTHING
// write()
    }

    /**
     * Expert users can override this method for more complete control over the
     * <p>
     * execution of the Mapper.
     *
     * @param context
     * @throws IOException
     */
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            while (context.nextKeyValue()) {
                map(context.getCurrentKey(), context.getCurrentValue(), context);
                COUNT++;
            }
        } finally {
            cleanup(context);
        }
    }

    public class Context implements MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        InputSplit inputSplit;
        LongWritable KEYIN;
        Text VALUEIN;
        Text KEYOUT;
        LongWritable VALUEOUT;

        @Override
        public KEYIN getCurrentKey() {
            return (KEYIN) KEYIN;
        }

        @Override
        public VALUEIN getCurrentValue() {
            return (VALUEIN) VALUEIN;
        }

        @Override
        public boolean nextKeyValue() {
            Boolean flagBoolean = false;
            if (inputSplit.getLength() > COUNT) {
                flagBoolean = true;
                KEYIN = new LongWritable(COUNT);
                VALUEIN = new Text(inputSplit.list.get(COUNT));
            }
            return flagBoolean;
        }

        @Override
        public void write(KEYOUT k, VALUEOUT v) {
            String mapTempFile = new File("").getAbsolutePath();
            File file = new File(mapTempFile + "/temp", "map.temp." + mapperId);
            GenFile.writeFile(file.getPath(), k + "", v + "");
//   LOG.info("map输出 key=" + k);
//   LOG.info("map输出 value=" + v);
        }

        public InputSplit getInputSplit() {
            return inputSplit;
        }

        public void setInputSplit(InputSplit inputSplit) {
            this.inputSplit = inputSplit;
        }
    }

    public interface MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        public KEYIN getCurrentKey();

        public VALUEIN getCurrentValue();

        public boolean nextKeyValue();

        public void write(KEYOUT k, VALUEOUT v);
    }
}