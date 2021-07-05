package com.abc.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Author: Cliff
 * Desc: 日期转换工具类
 * JDK8 的DateTimeFormatter 替换SimpleDateFormat，因为SimpleDateFormat 存在线程安全问题
 */
public class DateTimeUtil {
    private final static DateTimeFormatter formator = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(),
                ZoneId.systemDefault());
        return formator.format(localDateTime);
    }

    public static Long toTs(String YmDHms) {
        // System.out.println ("YmDHms:"+YmDHms);
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formator);
        long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }
}