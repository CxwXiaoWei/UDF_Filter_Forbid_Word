package com.missevan.demeter.UDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class FiterForbidWord extends GenericUDF {
    // 读取 csv 文件到数组
    public static ArrayList<String> regexps = new ArrayList<>();

    static {
        try (InputStream inputStream = FiterForbidWord.class.getClassLoader().getResourceAsStream("base_blacklist.csv");
             BufferedReader readCSV = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));) {
            String line;
            while ((line = readCSV.readLine()) != null) {
                String[] splits = line.split(",");
                if ("1".equals(splits[2])) {
                    // 1 为模糊匹配
                    regexps.add("(.*?)" + splits[0].substring(1, splits[0].length() - 1) + "(.*?)");
                } else if ("2".equals(splits[2])) {
                    // 2 为正则
                    regexps.add(splits[0].substring(1, splits[0].length() - 1));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        // 1.检查参数个数
        if (args.length != 1) {
            throw new UDFArgumentException("Param must be 1 argu.");
        }
        // 2.检查参数类型
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1, "A string argument was expected.");
        }
        // 3.检查参数每个元素类
        PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory();
        if (primitiveCategory != PrimitiveObjectInspector.PrimitiveCategory.STRING
                && primitiveCategory != PrimitiveCategory.CHAR
                && primitiveCategory != PrimitiveCategory.VARCHAR
                && primitiveCategory != PrimitiveCategory.VOID) {
            throw new UDFArgumentTypeException(1,
                    "A string, char, varchar or null argument was expected");
        }
        // 返回 int 类型
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        // 参数判空
        if (args[0].get() == null) {
            return null;
        }
        // 用来收集错误的正则
        // ArrayList<String> wrongRegexps= new ArrayList<>();
        for (String regexp : regexps) {
            try {
                boolean isMatch = Pattern.matches(regexp, args[0].get().toString());
                // 匹配到一个就退出
                if (isMatch) return 1;
            } catch (Exception e) {
                // 这里会有正则匹配错误 目前暂时不管，异常都跳过
                //wrongRegexps.add(regexp);
            }
        }
        return 0;
    }

    @Override
    public String getDisplayString(String[] strings) {
        // 生成HQL explain子句中显示的日志
        return strings[0];
    }

    // 测试
    public static void main(String[] args) {
        // 测试字符串
        String text = "cxw";
        if (args.length == 1) {
            text = args[0];
        }
        int result = 0;// 默认没有匹配到
        ArrayList<String> wrongRegexps = new ArrayList<>();
        for (String regexp : regexps) {
            try {
                boolean isMatch = Pattern.matches(regexp, text);
                if (isMatch) {
                    // 匹配到一个就退出
                    result = 1;
                    break;
                }
            } catch (Exception e) {
                wrongRegexps.add(e.toString());
            }
        }
        System.out.println(result);
//        for (int i = 0; i < wrongRegexps.size(); i++) {
//            System.out.println(wrongRegexps.get(i));
//        }
    }
}