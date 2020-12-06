package com.amane.bean.database;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Data
public class PaperIndex implements Writable {

    private long pid;

    private String title;

    private String doi;

    public PaperIndex() {

    }

    public PaperIndex(JSONObject jsonObject) {
        this.pid = jsonObject.getInteger("id");
        String title = jsonObject.getString("title");
        this.title = Objects.isNull(title) ? "" : title;
        String doi = jsonObject.getString("doi");
        this.doi = Objects.isNull(doi) ? "" : doi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(pid);
        dataOutput.writeUTF(title);
        dataOutput.writeUTF(doi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.pid = dataInput.readLong();
        this.title = dataInput.readUTF();
        this.doi = dataInput.readUTF();
    }

    public Map<String, String> asMap() {
        return new HashMap<String, String>() {
            {
                put("id", String.valueOf(pid));
                put("title", title);
                put("doi", doi);
            }
        };
    }
}