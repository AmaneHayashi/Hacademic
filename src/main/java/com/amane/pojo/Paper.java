package com.amane.pojo;

import lombok.Data;

import java.util.List;

/**
 * @author Amane Hayaashi
 * @date 2020/11/27
 * @since 1.0
 */

@Data
public class Paper {

    private long id;

    private List<Author> authors;

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

    private String doi;

    private String pdf;

    private String url;

    private List<String> references;

    private String Abstract;

    private IndexedAbstract indexedAbstract;

    private List<Fos> fos;

    private Venue venue;

}
