package com.amane.bean.database;

import com.amane.tools.BeanTools;
import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.stream.Collectors;

@Data
public class Paper implements Writable {

    private long pid;

    private String authors;

    private String title;

    private String keywords;

    private String docType;

    private int year;

    private int nCitation;

    private String doi;

    private String publisher;

    public Paper() {

    }

    public Paper(PaperJson paperJson) {
        String k1 = BeanTools.nullAsEmpty(paperJson.getKeywords()) + ",";
        try {
            authors = paperJson.getAuthors().stream().map(Author::getName).collect(Collectors.joining(","));
        } catch (Exception e) {
            authors = "";
        }
        try {
            String k2 = paperJson.getFos().stream().map(Fos::getName).collect(Collectors.joining(","));
            keywords = k1 + k2;
        } catch (Exception e) {
            keywords = k1;
        }
        this.pid = paperJson.getId();
        this.title = BeanTools.nullAsEmpty(paperJson.getTitle());
        this.docType = BeanTools.nullAsEmpty(paperJson.getDocType());
        this.year = paperJson.getYear();
        this.nCitation = paperJson.getNCitation();
        this.doi = BeanTools.nullAsEmpty(paperJson.getDoi());
        this.publisher = BeanTools.nullAsEmpty(paperJson.getPublisher());

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(pid);
        dataOutput.writeUTF(authors);
        dataOutput.writeUTF(title);
        dataOutput.writeUTF(keywords);
        dataOutput.writeUTF(docType);
        dataOutput.writeInt(year);
        dataOutput.writeInt(nCitation);
        dataOutput.writeUTF(doi);
        dataOutput.writeUTF(publisher);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pid = dataInput.readLong();
        authors = dataInput.readUTF();
        title = dataInput.readUTF();
        keywords = dataInput.readUTF();
        docType = dataInput.readUTF();
        year = dataInput.readInt();
        nCitation = dataInput.readInt();
        doi = dataInput.readUTF();
        publisher = dataInput.readUTF();
    }
}
