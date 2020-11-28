package com.amane.pojo;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Amane Hayaashi
 * @date 2020/11/28
 * @since 1.0
 */

@Data
@NoArgsConstructor
public class PaperWritable implements Writable {

    private long id;
    /*
        private String authors;

        private String title;

        private String keywords;

        private int year;

        private int nCitation;

        private String pageStart;

        private String pageEnd;

        private String docType;

        private String lang;

        private String publisher;

        private String volume;

        private String issue;

        private String issn;

        private String isbn;
    */
    private String doi;

    /*
        private String pdf;

        private String url;

        private String references;

        private String Abstract;

        private String indexedAbstract;

        private String fos;

        private String venue;
    */
    public PaperWritable(Paper paper) {
        this.id = paper.getId();
        this.doi = paper.getDoi();
        /*try {
            BeanUtils.copyProperties(this, paper);
            this.setAuthors(JSON.toJSONString(paper.getAuthors()));
            this.setReferences(JSON.toJSONString(paper.getReferences()));
            this.setIndexedAbstract(JSON.toJSONString(paper.getIndexedAbstract()));
            this.setFos(JSON.toJSONString(paper.getFos()));
            this.setVenue(JSON.toJSONString(paper.getVenue()));
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }*/
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        /*dataOutput.writeUTF(authors);
        dataOutput.writeUTF(title);
        dataOutput.writeUTF(keywords);
        dataOutput.writeInt(year);
        dataOutput.writeInt(nCitation);
        dataOutput.writeUTF(pageStart);
        dataOutput.writeUTF(pageEnd);
        dataOutput.writeUTF(docType);
        dataOutput.writeUTF(lang);
        dataOutput.writeUTF(publisher);
        dataOutput.writeUTF(volume);
        dataOutput.writeUTF(issue);
        dataOutput.writeUTF(issn);
        dataOutput.writeUTF(isbn);*/
        dataOutput.writeUTF(doi);/*
        dataOutput.writeUTF(pdf);
        dataOutput.writeUTF(url);
        dataOutput.writeUTF(references);
        dataOutput.writeUTF(Abstract);
        dataOutput.writeUTF(indexedAbstract);
        dataOutput.writeUTF(fos);
        dataOutput.writeUTF(venue);*/
    }

    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();/*
        authors = dataInput.readUTF();
        title = dataInput.readUTF();
        keywords = dataInput.readUTF();
        year = dataInput.readInt();
        nCitation = dataInput.readInt();
        pageStart = dataInput.readUTF();
        pageEnd = dataInput.readUTF();
        docType = dataInput.readUTF();
        lang = dataInput.readUTF();
        publisher = dataInput.readUTF();
        volume = dataInput.readUTF();
        issue = dataInput.readUTF();
        issn = dataInput.readUTF();
        isbn = dataInput.readUTF();*/
        doi = dataInput.readUTF();/*
        pdf = dataInput.readUTF();
        url = dataInput.readUTF();
        references = dataInput.readUTF();
        Abstract = dataInput.readUTF();
        indexedAbstract = dataInput.readUTF();
        fos = dataInput.readUTF();
        venue = dataInput.readUTF();*/
    }
}
