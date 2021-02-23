package com.info.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class GenFile {
    private static Logger logger = Logger.getLogger(GenFile.class);

    /**
     * read file
     *
     * @param src
     * @return
     */
    public static List<String> readFile(String src) {
        List<String> list = new ArrayList<String>();
        try {
            FileInputStream fis = new FileInputStream(new File(src));
            InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            String temp;
            while ((temp = br.readLine()) != null)
                list.add(temp.trim());
            fis.close();
            isr.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * write file
     *
     * @param des
     * @param k
     * @param v
     * @return
     */
    public static boolean writeFile(String des, String k, String v) {
        try {
            File file = new File(des);
            FileWriter writer = new FileWriter(file.getPath(), true);
            writer.write(k + "#&@" + v + '\n');
            writer.close();
        } catch (Exception e) {
            logger.info(e.getMessage(), e);
            return false;
        }
        return true;
    }
}