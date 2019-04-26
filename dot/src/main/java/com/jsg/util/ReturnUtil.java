package com.jsg.util;

import org.apache.commons.lang3.StringUtils;
import org.springframework.ui.ModelMap;

import java.util.HashMap;
import java.util.Map;

/**
 * JSON统一返回数据格式
 */
public class ReturnUtil {

    public static ModelMap Success(String msg, Object obj, String referer,
                                   int pageSize, int pageNo, int totalPage,
                                   long totalCount) {
        msg = StringUtils.isEmpty(msg) || StringUtils.isBlank(msg) ? "操作成功" : msg;
        ModelMap mp = new ModelMap();
        mp.put("status", 1);
        mp.put("state", "success");
        mp.put("msg", msg);
        mp.put("referer", referer);
        mp.put("result", obj);
        Map pageInfo=new HashMap();
        pageInfo.put("pageSize",pageSize);
        pageInfo.put("pageNo",pageNo);
        pageInfo.put("totalPage",totalPage);
        pageInfo.put("totalCount",totalCount);
        mp.put("page", pageInfo);
        return mp;
    }

    public static ModelMap Success(String msg, Object obj, String referer) {
        msg = StringUtils.isEmpty(msg) || StringUtils.isBlank(msg) ? "操作成功" : msg;
        ModelMap mp = new ModelMap();
        mp.put("status", 1);
        mp.put("state", "success");
        mp.put("msg", msg);
        mp.put("referer", referer);
        mp.put("result", obj);
        return mp;
    }

    public static ModelMap Success(String msg, Object obj) {
        msg = StringUtils.isEmpty(msg) || StringUtils.isBlank(msg) ? "操作成功" : msg;
        ModelMap mp = new ModelMap();
        mp.put("status", 1);
        mp.put("state", "success");
        mp.put("msg", msg);
        mp.put("referer", null);
        mp.put("result", obj);
        return mp;
    }
    public static ModelMap Success(String msg) {
        msg = StringUtils.isEmpty(msg) || StringUtils.isBlank(msg) ? "操作成功" : msg;
        ModelMap mp = new ModelMap();
        mp.put("status", 1);
        mp.put("state", "success");
        mp.put("msg", msg);
        mp.put("referer", null);
        mp.put("result", null);
        return mp;
    }

    public static ModelMap Error(String msg, Object obj, String referer) {
        msg = StringUtils.isEmpty(msg) || StringUtils.isBlank(msg) ? "操作失败" : msg;
        ModelMap mp = new ModelMap();
        mp.put("status", 0);
        mp.put("state", "error");
        mp.put("msg", msg);
        mp.put("referer", referer);
        mp.put("result", obj);
        return mp;
    }

    public static ModelMap Error(String msg, Object obj) {
        msg = StringUtils.isEmpty(msg) || StringUtils.isBlank(msg) ? "操作失败" : msg;
        ModelMap mp = new ModelMap();
        mp.put("status", 0);
        mp.put("state", "error");
        mp.put("msg", msg);
        mp.put("referer", null);
        mp.put("result", obj);
        return mp;
    }

    public static ModelMap Error(String msg) {
        msg = StringUtils.isEmpty(msg) || StringUtils.isBlank(msg) ? "操作失败" : msg;
        ModelMap mp = new ModelMap();
        mp.put("status", 0);
        mp.put("state", "error");
        mp.put("msg", msg);
        mp.put("referer", null);
        mp.put("result", null);
        return mp;
    }
}
