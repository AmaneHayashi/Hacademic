package com.amane.demo;

import com.amane.adapter.MRAdapter;
import com.amane.bean.database.PaperBackend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class SearchDemo {

    public static void main(String[] args) throws Exception {
        // BasicConfigurator.configure()
        ;/*
        Long[] pidArr = new Long[]{1091L, 1674L, 5411L, 5781L, 6522L,
                12993L, 52192L, 73348L, 11L};
        List<Long> longs = new ArrayList<>(Arrays.asList(pidArr));
        Collections.sort(longs);
        System.out.println(longs);
        */
        System.out.println(Calendar.getInstance().getTime());
        Long[] pidArr = new Long[]{1091L, 1674L, 5411L, 5781L, 6522L,
                12993L, 52192L, 73348L, 113959914L, 114372804L};
        List<PaperBackend> paperBackends = MRAdapter.findBy(new ArrayList<>(Arrays.asList(pidArr)), PaperBackend.class);
        System.out.println(paperBackends);
        System.out.println(Calendar.getInstance().getTime());


    }
}
