package com.amane.pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author Amane Hayaashi
 * @date 2020/11/27
 * @since 1.0
 */

@Data
public class IndexedAbstract {

    private String IndexLength;

    private Map<String, List<Integer>> InvertedIndex;

}
