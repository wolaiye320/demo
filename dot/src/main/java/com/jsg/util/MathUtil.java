package com.jsg.util;

import java.math.BigDecimal;

public class MathUtil {


    public static  float AreduceB(float a,float b){
        BigDecimal b1=new BigDecimal(Float.toString(b));
        BigDecimal b2=new BigDecimal(Float.toString(a));
        return b2.subtract(b1).floatValue();
    }
}
