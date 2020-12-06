package com.amane.adapter;

import com.amane.bean.hadoop.HBaseRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HBaseAdapter {

    private static final String DEF_COL_FAMILY = "def";

    private static final Configuration HBaseConf;

    static {
        HBaseConf = new Configuration();
        HBaseConf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        HBaseConf.set("hbase.zookeeper.property.clientPort", "2181");
    }

    private Connection conn = null;
    private Admin admin = null;

    public HBaseAdapter() {
        try {
            this.conn = ConnectionFactory.createConnection(HBaseConf);
            this.admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTable(TableName tableName) throws Exception {
        if (!admin.tableExists(tableName)) {
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            //列描述器集合
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            //表描述器构造器
            ColumnFamilyDescriptorBuilder cdb =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(DEF_COL_FAMILY));
            //获得列描述器
            ColumnFamilyDescriptor cfd = cdb.build();
            columnFamilyDescriptors.add(cfd);
            //添加列族
            tdb.setColumnFamilies(columnFamilyDescriptors);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
        }
    }

    public void insert(TableName tableName, HBaseRow hBaseRow) throws Exception {
        Table table = conn.getTable(tableName);
        String rowKey = hBaseRow.getRowKey();
        Put put = new Put(Bytes.toBytes(rowKey));
        Map<String, Object> columnMap = hBaseRow.getColumnValues();
        for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
            put.addColumn(Bytes.toBytes(DEF_COL_FAMILY),
                    Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
        }
        table.put(put);
        table.close();
    }

    public void insert(TableName tableName, List<HBaseRow> hBaseRows) throws Exception {
        Table table = conn.getTable(tableName);
        List<Put> puts = new ArrayList<>();
        for (HBaseRow hBaseRow : hBaseRows) {
            String rowKey = hBaseRow.getRowKey();
            Put put = new Put(Bytes.toBytes(rowKey));
            Map<String, Object> columnMap = hBaseRow.getColumnValues();
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                put.addColumn(Bytes.toBytes(DEF_COL_FAMILY),
                        Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }

    public void update(TableName tableName, List<HBaseRow> hBaseRows) throws Exception {
        insert(tableName, hBaseRows);
    }

    public void deleteRow(TableName tableName, String rowKey) throws Exception {
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    public void deleteColumnFamily(TableName tableName, String rowKey, String columnFamily) throws IOException {
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //②删除某一行的某一个列簇内容
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.close();
    }

    public void deleteColumns(TableName tableName, String rowKey, String... columnNames) throws IOException {
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //③删除某一行某个列簇某列的值
        for (String columnName : columnNames) {
            delete.addColumn(Bytes.toBytes(DEF_COL_FAMILY), Bytes.toBytes(columnName));
        }
        table.close();
    }

    public void deleteTable(TableName tableName) throws IOException {
        admin.deleteTable(tableName);
    }

    public void deleteTableColumnFamily(TableName tableName, String columnFamily) throws IOException {
        admin.disableTable(tableName);
        admin.deleteColumnFamily(tableName, Bytes.toBytes(columnFamily));
    }

    public void truncateTable(TableName tableName) throws IOException {
        admin.truncateTable(tableName, true);
    }

    public HBaseRow queryByRowKey(TableName tableName, String rowKey) throws Exception {
        Table table = conn.getTable(tableName);
        HBaseRow hBaseRow = new HBaseRow(rowKey);
        //获得一行
        Get get = new Get(Bytes.toBytes(rowKey));
        Result set = table.get(get);
        if (set.isEmpty()) {
            return null;
        }
        Cell[] cells = set.rawCells();
        hBaseRow.setColumnValues(cells);
        table.close();
        return hBaseRow;
    }

    public List<HBaseRow> queryByFilter(TableName tableName, Filter filter) throws Exception {
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner results = table.getScanner(scan);
        List<HBaseRow> hBaseRows = new ArrayList<>();
        for (Result rs : results) {
            String rowKey = Bytes.toString(rs.getRow());
            HBaseRow hBaseRow = new HBaseRow(rowKey);
            Cell[] cells = rs.rawCells();
            hBaseRow.setColumnValues(cells);
            hBaseRows.add(hBaseRow);
        }
        table.close();
        return hBaseRows;
    }

    public void queryByFilterDemo(TableName tableName) throws Exception {
        //  不设置过滤器 - 全盘扫描
        queryByFilter(tableName, null);
        /*  行过滤器 - 根据行键过滤
            第一个参数为过滤规则，包括：
                LESS（<），LESS_OR_EQUAL（<=），EQUAL（==），NOT_EQUAL（!=），
                GREATER_OR_EQUAL（>=），GREATER（>），NO_OP（排除所有符合条件的值）
            第二个参数为比较器，包括：
                BinaryComparator（字典序比较），BinaryPrefixComparator（字典序前缀比较），RegexStringComparator（正则比较，只支持EQUAL/NOT_EQUAL），
                SubStringComparator（包含比较，只支持EQUAL/NOT_EQUAL），NullComparator（判空比较），BitComparator（按位与/或/异或比较）
            正则比较的注意事项：
                str$为末尾匹配，相当于相当于sql中的%str；
                ^str为开头匹配，相当于sql中的str%
        */
        queryByFilter(tableName, new RowFilter(CompareOperator.LESS,
                new BinaryComparator(Bytes.toBytes("abbc"))));
        queryByFilter(tableName, new RowFilter(CompareOperator.LESS_OR_EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("abbc"))));
        queryByFilter(tableName, new RowFilter(CompareOperator.EQUAL,
                new RegexStringComparator("abc$")));
        queryByFilter(tableName, new RowFilter(CompareOperator.NOT_EQUAL,
                new SubstringComparator("abcd")));
        queryByFilter(tableName, new RowFilter(CompareOperator.NO_OP,
                new NullComparator()));
        queryByFilter(tableName, new RowFilter(CompareOperator.EQUAL,
                new BitComparator(new byte[]{0, 0, 0, 0}, BitComparator.BitwiseOp.XOR)));
        //  列簇过滤器 - 根据列簇的名字过滤
        queryByFilter(tableName, new FamilyFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("name"))));
        //  列过滤器 - 根据列的名字过滤
        queryByFilter(tableName, new QualifierFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("name"))));
        //  值过滤器 - 根据任意一列的值过滤
        queryByFilter(tableName, new ValueFilter(CompareOperator.EQUAL,
                new SubstringComparator("123")));
        //  时间戳过滤器 - 根据时间戳过滤
        queryByFilter(tableName, new TimestampsFilter(
                Collections.singletonList(System.currentTimeMillis())));
        /*  单列值过滤器 - 根据某一列的值过滤
            需要传递4个参数，分别为列簇名、列名、过滤规则、过滤器
        */
        queryByFilter(tableName, new SingleColumnValueFilter(Bytes.toBytes("name"),
                Bytes.toBytes("name"), CompareOperator.EQUAL, new SubstringComparator("hadoop")));
        //  单列值排除器 - 根据某一列的值排除
        queryByFilter(tableName, new SingleColumnValueExcludeFilter(Bytes.toBytes("name"),
                Bytes.toBytes("name"), CompareOperator.EQUAL, new SubstringComparator("hadoop")));
        //  行前缀过滤器 - 根据行键前缀过滤
        queryByFilter(tableName, new PrefixFilter(Bytes.toBytes("1234")));
        //  列前缀过滤器 - 根据列前缀过滤
        queryByFilter(tableName, new ColumnPrefixFilter(Bytes.toBytes("name")));
        //  分页过滤器 - 根据行数过滤（前X行）
        queryByFilter(tableName, new PageFilter(20));
        /*  多过滤器 - 多重过滤器过滤
            MUST_PASS_ALL - 全部通过才通过，MUST_PASS_ONE - 通过一个就通过
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        RowFilter filter1 = new RowFilter(CompareOperator.NOT_EQUAL,
                new SubstringComparator("abcd"));
        ColumnPrefixFilter filter2 = new ColumnPrefixFilter(Bytes.toBytes("name"));
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        queryByFilter(tableName, filterList);
    }
}
