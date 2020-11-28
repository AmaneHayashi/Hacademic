package com.amane.demo;

import com.alibaba.fastjson.JSONReader;
import com.amane.pojo.Paper;
import com.amane.pojo.PaperWritable;

import java.io.FileReader;
import java.io.IOException;

/**
 * @author Amane Hayaashi
 * @date 2020/11/27
 * @since 1.0
 */

public class JsonConverter {

    public static void main(String[] args) throws IOException {
        String path = "D:\\CodeTemp\\dblp.v12\\paper.json";
        JSONReader reader = new JSONReader(new FileReader(path));
        reader.startArray();
        while (reader.hasNext()) {
            Paper paper = reader.readObject(Paper.class);
            PaperWritable paperWritable = new PaperWritable(paper);
            System.out.println(paperWritable.getDoi());
        }
        reader.endArray();
        reader.close();
    }
}
