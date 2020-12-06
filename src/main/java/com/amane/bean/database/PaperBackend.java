package com.amane.bean.database;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.esotericsoftware.minlog.Log;
import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Data
public class PaperBackend implements Writable {

    private long pid;

    private String title;

    private String authors;

    private int year;

    private String publisher;

    public PaperBackend() {

    }

    public PaperBackend(JSONObject jsonObject) {
        this.pid = jsonObject.getInteger("id");
        String title = jsonObject.getString("title");
        this.title = Objects.isNull(title) ? "" : title;
        this.year = jsonObject.getInteger("year");
        String Abstract = jsonObject.getString("abstract");
        this.publisher = Objects.isNull(Abstract) ? "" : Abstract;
        try {
            JSONArray authors = jsonObject.getJSONArray("authors");
            this.authors = authors.toJavaList(Author.class).stream().map(Author::getName).collect(Collectors.joining(","));
        } catch (Exception e) {
            this.authors = "";
            Log.error(e.getMessage());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(pid);
        dataOutput.writeUTF(title);
        dataOutput.writeUTF(authors);
        dataOutput.writeInt(year);
        dataOutput.writeUTF(publisher);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pid = dataInput.readLong();
        this.title = dataInput.readUTF();
        this.authors = dataInput.readUTF();
        this.year = dataInput.readInt();
        this.publisher = dataInput.readUTF();
    }

    public Map<String, String> asMap() {
        return new HashMap<String, String>() {
            {
                put("id", String.valueOf(pid));
                put("title", title);
                put("author", authors);
                put("year", String.valueOf(year));
                put("abstract", publisher);
            }
        };
    }
}
