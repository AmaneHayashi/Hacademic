package com.amane.bean.backend;

import com.alibaba.fastjson.JSONObject;
import com.amane.enumurate.MessageEnum;
import lombok.Data;

import java.io.Serializable;

@Data
public class Response implements Serializable {

    private String message;

    private String log;

    private Object result;

    public void setMessage(MessageEnum messageEnum) {
        this.message = messageEnum.getMessage();
    }

    public byte[] toJSONBytes() {
        return JSONObject.toJSONString(this).getBytes();
    }

}
