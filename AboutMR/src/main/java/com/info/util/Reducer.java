package com.info.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

@SuppressWarnings("unchecked")
public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    public Logger log = Logger.getLogger(Reducer.class);
    public String reduceId = UUID.randomUUID().toString();
    public int COUNT = 0;

    /**
     * The <code>Context</code> passed on to the {@link Reducer}
     * <p>
     * implementations.
     */
    public class Context implements ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        Map<Object, List<Object>> dataMap;
        List<Object> dataKey;
        Text KEYIN;
        LongWritable VALUEIN;
        Text KEYOUT;
        Text VALUEOUT;

        @Override
        public KEYIN getCurrentKey() throws IOException, InterruptedException {
// TODO Auto-generated method stub
            return (KEYIN) KEYIN;
        }

        @Override
        public VALUEIN getCurrentValue() throws IOException,
                InterruptedException {
// TODO Auto-generated method stub
            return (VALUEIN) VALUEIN;
        }

        @Override
        public List<VALUEIN> getValues() throws IOException,
                InterruptedException {
            return (List<VALUEIN>) dataMap.get(dataKey.get(COUNT));
        }

        @Override
        public boolean nextKey() throws IOException, InterruptedException {
// TODO Auto-generated method stub
            Boolean flagBoolean = false;
            if (dataMap.size() > COUNT) {
                flagBoolean = true;
                KEYIN = new Text((String) dataKey.get(COUNT));
            }
            return flagBoolean;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
// TODO Auto-generated method stub
            return false;
        }

        @Override
        public void write(KEYOUT key, VALUEOUT value) throws IOException,
                InterruptedException {
// TODO Auto-generated method stub
            log.info("key=" + key);
            log.info("value=" + value);
        }

        public Map<Object, List<Object>> getDataMap() {
            return dataMap;
        }

        public void setDataMap(Map<Object, List<Object>> dataMap) {
            this.dataMap = dataMap;
        }

        public List<Object> getDataKey() {
            return dataKey;
        }

        public void setDataKey(List<Object> dataKey) {
            this.dataKey = dataKey;
        }
    }

    /**
     * Called once at the start of the task.
     */

    protected void setup(Context context) throws IOException,
            InterruptedException {
// NOTHING
    }

    /**
     * This method is called once for each key. Most applications will define
     * <p>
     * their reduce class by overriding this method. The default implementation
     * <p>
     * is an identity function.
     */
    protected void reduce(KEYIN key, List<VALUEIN> values, Context context)
            throws IOException, InterruptedException {
        for (VALUEIN value : values) {
            context.write((KEYOUT) key, (VALUEOUT) value);
        }
    }

    /**
     * Called once at the end of the task.
     */
    protected void cleanup(Context context) throws IOException,
            InterruptedException {
// NOTHING
    }

    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            while (context.nextKey()) {
                reduce(context.getCurrentKey(), context.getValues(), context);
                COUNT++;
            }
        } finally {
            cleanup(context);
        }
    }

    public interface ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
        /**
         * Advance to the next key, value pair, returning null if at end.
         *
         * @return the key object that was read into, or null if no more
         */
        public boolean nextKeyValue() throws IOException, InterruptedException;

        /**
         * Get the current key.
         *
         * @return the current key object or null if there isn't one
         * @throws IOException
         * @throws InterruptedException
         */
        public KEYIN getCurrentKey() throws IOException, InterruptedException;

        /**
         * Get the current value.
         *
         * @return the value object that was read into
         * @throws IOException
         * @throws InterruptedException
         */
        public VALUEIN getCurrentValue() throws IOException,
                InterruptedException;

        /**
         * Generate an output key/value pair.
         */
        public void write(KEYOUT key, VALUEOUT value) throws IOException,
                InterruptedException;

        /**
         * Start processing next unique key.
         */
        public boolean nextKey() throws IOException, InterruptedException;

        /**
         * Iterate through the values for the current key, reusing the same
         * <p>
         * value object, which is stored in the context.
         *
         * @return the series of values associated with the current key. All of
         * <p>
         * the objects returned directly and indirectly from this method
         * <p>
         * are reused.
         */
        public Iterable<VALUEIN> getValues() throws IOException,
                InterruptedException;

        /**
         * {@link Iterator} to iterate over values for a given group of records.
         */
        interface ValueIterator<VALUEIN> extends Iterator<VALUEIN> {
            /**
             * This method is called when the reducer moves from one key to
             * <p>
             * another.
             *
             * @throws IOException
             */
            void resetBackupStore() throws IOException;

            /**
             * Mark the current record. A subsequent call to reset will rewind
             * <p>
             * the iterator to this record.
             *
             * @throws IOException
             */
            void mark() throws IOException;

            /**
             * Reset the iterator to the last record before a call to the
             * <p>
             * previous mark
             *
             * @throws IOException
             */
            void reset() throws IOException;

            /**
             * Clear any previously set mark
             *
             * @throws IOException
             */
            void clearMark() throws IOException;
        }
    }
}