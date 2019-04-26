package com.jsg.util;

/**
 * 字符串工具
 */
public class StringUtil {


    /**
     * 逗号分隔字符串加上单引号
     * @param str
     * @return
     */
    public static String spilt(String str) {
        StringBuffer sb = new StringBuffer();
        String[] temp = str.split(",");
        for (int i = 0; i < temp.length; i++) {
            if (!"".equals(temp[i]) && temp[i] != null)
                sb.append("'" + temp[i] + "',");
        }
        String result = sb.toString();
        String tp = result.substring(result.length() - 1, result.length());
        if (",".equals(tp))
            return result.substring(0, result.length() - 1);
        else
            return result;
    }

    public static String spiltIndex(String str) {

        if(null==str){
            return null;
        }
        if("".equals(str)){
            return "";
        }
        if(str.indexOf("'")!=-1){
            return str;
        }
        StringBuffer sb = new StringBuffer();
        String[] temp = str.split(",");
        for (int i = 0; i < temp.length; i++) {
            if (!"".equals(temp[i]) && temp[i] != null)
                sb.append("'" + temp[i] + "',");
        }
        String result = sb.toString();
        String tp = result.substring(result.length() - 1, result.length());
        if (",".equals(tp))
            return result.substring(0, result.length() - 1);
        else
            return result;
    }
}
