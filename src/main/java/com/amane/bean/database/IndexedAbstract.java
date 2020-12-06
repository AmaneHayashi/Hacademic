package com.amane.bean.database;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class IndexedAbstract {

    private String IndexLength;

    private Map<String, List<Integer>> InvertedIndex;

}
