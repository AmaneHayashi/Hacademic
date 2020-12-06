package com.amane.meta;

import com.alibaba.fastjson.JSONReader;
import com.amane.adapter.HBaseAdapter;
import com.amane.bean.backend.User;
import com.amane.consts.ConstValue;
import org.apache.log4j.BasicConfigurator;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class MetaHBase {

    private static final String USER_INFO_PATH = "D:\\CodeTemp\\dblp.v12\\user.json";

    public static void init() {
        BasicConfigurator.configure();
        try {
            HBaseAdapter hBaseAdapter = new HBaseAdapter();
            // 创建用户信息表
            hBaseAdapter.createTable(ConstValue.USER_INFO_TABLE);
            // 创建用户行为表
            hBaseAdapter.createTable(ConstValue.USER_BEHAVIOR_TABLE);
            // 创建用户推荐表
            hBaseAdapter.createTable(ConstValue.USER_RECOMMEND_TABLE);
            // 创建论文后台关联表
            hBaseAdapter.createTable(ConstValue.PAPER_BACKEND_TABLE);
            // 创建论文索引表
            hBaseAdapter.createTable(ConstValue.PAPER_INDEX_TABLE);
            // 创建论文推荐表
            hBaseAdapter.createTable(ConstValue.PAPER_RECOMMEND_TABLE);
            /*
            // 添加用户信息表信息
            List<User> userList = readUserInfo();
            hBaseAdapter.insert(Constants.USER_INFO_TABLE, userList.stream()
                    .map(u -> new HBaseRow(u.getName(), BeanTools.toMap(u)))
                    .collect(Collectors.toList()));
            // 添加用户推荐表信息
            hBaseAdapter.insert(Constants.USER_RECOMMEND_TABLE, userList.stream()
                    .map(u -> new HBaseRow(u.getName(), null))
                    .collect(Collectors.toList()));

             */
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<User> readUserInfo() throws FileNotFoundException {
        List<User> userList = new ArrayList<>();
        JSONReader reader = new JSONReader(new FileReader(USER_INFO_PATH));
        reader.startArray();
        while (reader.hasNext()) {
            User user = reader.readObject(User.class);
            userList.add(user);
        }
        reader.endArray();
        reader.close();
        return userList;
    }
}
