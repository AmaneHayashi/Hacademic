package com.amane.adapter;

import com.amane.rkmd.Recommend;

import java.util.Map;

public class RkmdAdapter {

    private static long RTimestamp = 0L;

    public static boolean isNewestURkmd() throws Exception {
        return Recommend.isNewest(RTimestamp);
    }

    public static Map<String, String> executeURkmd() throws Exception {
        Map<String, String> map = Recommend.execute();
        RTimestamp = System.currentTimeMillis();
        Recommend.updateTimestamp(RTimestamp);
        return map;
    }

}
