package com.amane.demo;

import com.amane.adapter.HDFSAdapter;
import com.amane.consts.ConstValue;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class TestDemo {

    static String[] name = new String[]{
            "output_1607165939393",
            "output_1607166086833",
            "output_1607166438750",
            "output_1607166553914",
            "output_1607166619688",
            "output_1607166860894",
            "output_1607166936865",
            "output_1607167023939",
            "output_1607167096973",
            "output_1607167189747",
            "output_1607167256449",
            "output_1607167460704",
            "output_1607168031941",
            "output_1607168176627",
            "output_1607168211797",
            "output_1607168426073",
            "output_1607168482805",
            "output_1607168532245",
            "output_1607168580355",
            "output_1607168607872",
            "output_1607168829267",
            "output_1607169138125",
            "output_1607169281081",
            "output_1607170052081",
            "output_1607170178274",
            "output_1607170268035",
            "output_1607170323349",
            "output_1607170396442",
            "output_1607170536409",
            "output_1607170767774",
            "output_1607171726997",
            "output_1607171768363",
            "output_1607171801487",
            "output_1607172318482",
            "output_1607172349480",
            "output_1607172409275",
            "output_1607172482944",
            "output_1607172788395",
            "output_1607172928269"
    };

    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        HDFSAdapter hdfsAdapter = new HDFSAdapter();
        for (String s : name) {
            hdfsAdapter.deleteFile(ConstValue.MASTER_HDFS + "/" + s);
        }

    }

}
