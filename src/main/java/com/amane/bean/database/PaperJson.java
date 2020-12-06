package com.amane.bean.database;

import lombok.Data;

import java.util.List;

/**
 * @author Amane Hayaashi
 * @date 2020/12/5
 * @since 1.0
 */

@Data
public class PaperJson {

    private long id;

    private List<Author> authors;

    private String title;

    private String keywords;

    private String docType;

    private List<Fos> fos;

    private int year;

    private int nCitation;

    private String doi;

    private String publisher;

}
