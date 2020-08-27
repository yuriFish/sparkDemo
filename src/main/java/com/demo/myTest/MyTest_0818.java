package com.demo.myTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MyTest_0818 {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
//        List<String> list = new CopyOnWriteArrayList<>();
        list.add("沉默王二");
        list.add("沉默王三");
        list.add("一个文章真特么有趣的程序员");

        // 报错ConcurrentModificationException 因为使用了迭代器
//        for (String str : list) {
//            if ("沉默王二".equals(str)) {
//                list.remove(str);
//            }
//        }
        // 不报错
//        for (int i = 0; i < list.size(); i++) {
//            if ("沉默王二".equals(list.get(i))) {
//                list.remove(i);
//            }
//        }

//        System.out.println(list);
        System.out.println(0%2);
    }
}
