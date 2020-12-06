package com.amane.tools;

import org.apache.commons.beanutils.BeanUtils;

import java.util.*;
import java.util.stream.Collectors;

public class BeanTools {

    public static Map<String, String> toMap(Object pojo) {
        try {
            return BeanUtils.describe(pojo);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String nullAsEmpty(String s) {
        return Objects.isNull(s) ? "" : s;
    }

    public static boolean anyContains(String feed, List<Long> tar) {
        int k = 0;
        for (int i = 0; i < tar.size(); i++) {
            long l = tar.get(i);
            String s = String.format("\"%d\"", l);
            if (feed.contains(s)) {
                while (k++ < i) {
                    tar.remove(tar.get(0));
                }
                return true;
            }
        }
        return false;
    }

    public static String mergeJoin(String separator, String... orgs) {
        List<String> list = new ArrayList<>();
        for (String s : orgs) {
            String[] arr = s.split(separator);
            list.addAll(Arrays.asList(arr));
        }
        return list.stream().distinct().collect(Collectors.joining(separator));
    }

}
