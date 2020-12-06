package com.amane.demo;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;

public class ReaderDemo {

    public static final String FILE_PATH = "D:\\CodeTemp\\dblp.v12\\index.txt";

    private static final String TARGET_STRING = "On the Integration of Theoretical Single-Objective Scheduling Results for Multi-objective Problems";

    public static void main(String[] args) throws IOException {
        LineIterator lineIterator = FileUtils.lineIterator(new File(FILE_PATH), "utf-8");
        System.out.println(Calendar.getInstance().getTime());
        while (lineIterator.hasNext()) {
            String result = lineIterator.next();
            if (result.contains(TARGET_STRING)) {
                System.out.println(result.substring(0, result.indexOf("|")));
                break;
            }
        }
        System.out.println(Calendar.getInstance().getTime());
    }
}
