package com.amane.bean.hadoop;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.hbase.Cell;

import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HBaseRow {

    private String rowKey;

    private Map<String, Object> columnValues;

    public HBaseRow(String key) {
        this.rowKey = key;
    }

    public void setColumnValues(Map<String, Object> columnValues) {
        this.columnValues = columnValues;
    }

    public void setColumnValues(Cell[] cells) {
        Map<String, Object> columnValues = new HashMap<>();
        for (Cell cell : cells) {
            String columnName = new String(cell.getQualifierArray());
            String columnValue = new String(cell.getValueArray());
            columnValues.put(columnName, columnValue);
        }
        this.columnValues = columnValues;
    }
}
