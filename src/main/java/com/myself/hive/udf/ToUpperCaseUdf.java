package com.myself.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zxq
 * 2020/5/14
 */
public class ToUpperCaseUdf extends UDF {

    public String evaluate(String str){

        if (str==null){
            return null;
        }
        return str.toUpperCase();
    }

}
