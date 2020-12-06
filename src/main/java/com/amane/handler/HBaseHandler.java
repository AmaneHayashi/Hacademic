package com.amane.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.amane.adapter.HBaseAdapter;
import com.amane.adapter.HDFSAdapter;
import com.amane.adapter.MRAdapter;
import com.amane.adapter.RkmdAdapter;
import com.amane.bean.backend.Response;
import com.amane.bean.database.Paper;
import com.amane.bean.database.PaperBackend;
import com.amane.bean.hadoop.HBaseRow;
import com.amane.consts.ConstValue;
import com.amane.enumurate.MessageEnum;
import com.amane.rkmd.Recommend;
import com.amane.tools.BeanTools;
import com.amane.tools.MD5Tools;
import com.amane.tools.ReaderTools;
import com.amane.tools.RkmdTools;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class HBaseHandler {

    private static final Map<String, String> switchMap = new HashMap<String, String>() {
        {
            put("UserLogin", "isValidLogin");
            put("UserBehavior", "updateUserBehavior");
            put("HistoryPaper", "getHistories");
            put("RecommendPaper", "getRecommends");
        }
    };

    private static final HBaseAdapter hBaseAdapter;
    private static final HDFSAdapter hdfsAdapter;

    static {
        hBaseAdapter = new HBaseAdapter();
        hdfsAdapter = new HDFSAdapter();
    }

    /*  [返回消息格式]
        message : success / wrong / fail,
        log     : success -> null; wrong / fail -> logs,
        result  : getter -> results; setter -> null
     */
    public static byte[] handle(byte[] request) {
        String req = new String(request);
        Response response = new Response();
        if (JSON.isValid(req)) {
            JSONObject jsonObject = JSON.parseObject(req);
            String key = jsonObject.getString("method");
            if (!Objects.isNull(key)) {
                try {
                    String methodName = switchMap.get(key);
                    Method method = HBaseHandler.class.getMethod(methodName, JSONObject.class);
                    response = (Response) method.invoke(null, jsonObject);
                    return response.toJSONBytes();
                } catch (Exception e) {
                    e.printStackTrace();
                    response.setLog(e.getMessage());
                }
            } else {
                response.setLog("No method key in JSON");
            }
        } else {
            response.setLog("Input JSON string is invalid");
        }
        response.setMessage(MessageEnum.FAIL);
        response.setResult(null);
        return response.toJSONBytes();
    }

    /*  [传入消息格式]
        method  : UserLogin
        uid     : "admin"
        pswd    : "123456"
        [返回消息格式]
        message : "success",
        log     : null,
        result  : null
     */
    public Response isValidLogin(JSONObject request) throws Exception {
        Response response = new Response();
        String uid = request.getString("uid");
        String pswd = request.getString("pswd");
        HBaseRow hBaseRow = hBaseAdapter.queryByRowKey(ConstValue.USER_INFO_TABLE, uid);
        if (Objects.isNull(hBaseRow)) {
            response.setLog("no such user");
        } else {
            String pswdMD5 = hBaseRow.getColumnValues().get("pswd").toString();
            if (Objects.equals(MD5Tools.md5(pswd), pswdMD5)) {
                response.setMessage(MessageEnum.SUCCESS);
                response.setLog(null);
                response.setResult(null);
                return response;
            } else {
                response.setLog("wrong password");
            }
        }
        response.setMessage(MessageEnum.WRONG);
        response.setResult(null);
        return response;
    }

    /*  [传入消息格式]
        method  : UserBehavior
        uid     : "admin"
        title   : "TITLE NAME"
        [返回消息格式]
        message : "success",
        log     : null,
        result  : null
     */
    public Response updateUserBehavior(JSONObject request) throws Exception {
        Response response = new Response();
        // 拿到论文ID
        Paper paper = request.toJavaObject(Paper.class);
        String localIndexPath = ConstValue.LOCAL_FILE_DIR + "\\" + ConstValue.INDEX_FILE_NAME;
        String uid = request.getString("uid");
        String pid = String.valueOf(System.currentTimeMillis());
        String title = paper.getTitle();
        String index = ReaderTools.find(localIndexPath, title);
        // 如果论文ID不存在，则追加存储论文信息
        if (Objects.isNull(index)) {
            new Thread(() -> appendPaperAsync(paper)).start();
        } else {
            pid = index.substring(0, index.indexOf("|"));
        }
        // 拿到用户关键词
        HBaseRow rHBaseRow = hBaseAdapter.queryByRowKey(ConstValue.USER_RECOMMEND_TABLE, uid);
        String uKeywords = rHBaseRow.getColumnValues().get("keywords").toString();
        // 拿到其它参数
        int year = paper.getYear();
        int nCitation = paper.getNCitation();
        String pKeywords = paper.getKeywords();
        // 计算评分
        // double star = RkmdTools.executeStar(year, nCitation, pKeywords, uKeywords);
        double star = RkmdTools.getRandomStar();
        // 拼接Behavior表的Row
        String rowKey1 = String.format("%s-%s", uid, pid);
        Map<String, Object> map1 = new HashMap<>();
        map1.put("uid", uid);
        map1.put("pid", pid);
        map1.put("year", year);
        map1.put("nCitation", nCitation);
        map1.put("keywords", pKeywords);
        map1.put("star", String.valueOf(star));
        // 存储Behavior表
        HBaseRow hBaseRow1 = new HBaseRow();
        hBaseRow1.setRowKey(rowKey1);
        hBaseRow1.setColumnValues(map1);
        hBaseAdapter.update(ConstValue.USER_BEHAVIOR_TABLE, Collections.singletonList(hBaseRow1));
        // 拼接HDFS用户推荐表的数据
        String HDFSRkmdPath = ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_RKMD_DIR +
                "/" + ConstValue.RKMD_FILE_NAME;
        String metaRkmd = String.format("%s,%s,%s", uid, pid, star);
        // 存储HDFS
        hdfsAdapter.append(Bytes.toBytes(metaRkmd), HDFSRkmdPath);
        // 拼接Recommend表的Row
        String keywords = BeanTools.mergeJoin(",", pKeywords, uKeywords);
        Map<String, Object> map2 = new HashMap<>();
        map2.put("keywords", keywords);
        // 存储Recommend表
        HBaseRow hBaseRow2 = new HBaseRow();
        hBaseRow2.setRowKey(uid);
        hBaseRow2.setColumnValues(map2);
        hBaseAdapter.update(ConstValue.USER_BEHAVIOR_TABLE, Collections.singletonList(hBaseRow2));
        // 修改推荐的时间戳
        Recommend.updateTimestamp(System.currentTimeMillis());
        // 写Response并回传
        response.setMessage(MessageEnum.SUCCESS);
        response.setLog(null);
        response.setResult(null);
        return response;
    }

    /*  [返回消息格式]
        message : success,
        log     : null,
        result  : [pid List]
     */
    public Response getHistories(JSONObject request) throws Exception {
        Response response = new Response();
        String uid = request.getString("uid");
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        RowFilter filter1 = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes(uid)));
        PageFilter filter2 = new PageFilter(10);
        filterList.addFilter(Arrays.asList(filter1, filter2));
        List<HBaseRow> hBaseRows = hBaseAdapter.queryByFilter(ConstValue.USER_BEHAVIOR_TABLE, filterList);
        response.setMessage(MessageEnum.SUCCESS);
        response.setLog(null);
        List<Long> pidList = hBaseRows.stream().map(HBaseRow::getColumnValues)
                .map(h -> Long.parseLong(h.get("pid").toString())).collect(Collectors.toList());
        List<PaperBackend> paperBackends = MRAdapter.findBy(pidList, PaperBackend.class);
        response.setResult(paperBackends);
        return response;
    }

    /*  [返回消息格式]
        message : success,
        log     : null,
        result  : [pid List]
     */
    public Response getRecommends(JSONObject request) throws Exception {
        // 功能：推荐请求-判断时间戳-算法开始-输出数据转成HBase（时间戳确定法）-一路输出，一路写入HBase
        Response response = new Response();
        String uid = request.getString("uid");
        List<Long> pidList;
        // 如果推荐是最新的，直接取出
        if (RkmdAdapter.isNewestURkmd()) {
            HBaseRow hBaseRow = hBaseAdapter.queryByRowKey(ConstValue.USER_BEHAVIOR_TABLE, uid);
            Map<String, Object> columnValues = hBaseRow.getColumnValues();
            response.setMessage(MessageEnum.SUCCESS);
            response.setLog(null);
            pidList = columnValues.entrySet().stream().filter(s -> s.getKey().contains("recommend"))
                    .map(Map.Entry::getValue).map(Object::toString)
                    .map(Long::parseLong).collect(Collectors.toList());
        } else {
            // 否则，重新计算
            Map<String, String> URkmdMap = RkmdAdapter.executeURkmd();
            pidList = Arrays.stream(URkmdMap.get(uid).split(","))
                    .map(Long::parseLong).collect(Collectors.toList());
            // 异步存储
            new Thread(() -> storeRkmdAsync(URkmdMap)).start();
        }
        List<PaperBackend> paperBackends = MRAdapter.findBy(pidList, PaperBackend.class);
        response.setResult(paperBackends);
        return response;
    }

    private void appendPaperAsync(Paper paper) {
        String HDFSDataFilePath = ConstValue.MASTER_HDFS + "/" + ConstValue.HDFS_DATA_DIR
                + "/" + ConstValue.DATA_FILE_NAME;
        String s = '\n' + JSON.toJSONString(paper);
        try {
            hdfsAdapter.append(Bytes.toBytes(s), HDFSDataFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void storeRkmdAsync(Map<String, String> rkmdMap) {
        List<HBaseRow> hBaseRows = new ArrayList<>();
        for (Map.Entry<String, String> entry : rkmdMap.entrySet()) {
            HBaseRow hBaseRow = new HBaseRow();
            hBaseRow.setRowKey(entry.getKey());
            String[] val = entry.getValue().split(",");
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < val.length; i++) {
                map.put(String.format("recommend-u%d", i), val[i]);
            }
            hBaseRow.setColumnValues(map);
            hBaseRows.add(hBaseRow);
        }
        try {
            hBaseAdapter.insert(ConstValue.USER_RECOMMEND_TABLE, hBaseRows);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
