package com.amane.tools;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

public class ReaderTools {

    public static String find(String path, String target) throws IOException {
        LineIterator lineIterator = FileUtils.lineIterator(new File(path), "utf-8");
        System.out.println(Calendar.getInstance().getTime());
        while (lineIterator.hasNext()) {
            String s = lineIterator.next();
            if (s.contains(target)) {
                return s;
            }
        }
        return null;
    }
}
