package com.demo.myTest;

/**
 * 在主线程中开启3个子线程，分别实现打印字符串 012 345 678 (3) "ABC"-->"CBA"-->"BAC"
 * 三个线程ABC分别向一个数组中写入a，l，i，要求最终的写入结果形如alialiali...写入次数由A线程决定
 */
public class OpenThreeThread_three extends Thread {
    private static Object LOCK = new Object();
    private static int p = 0;
    private static int count = 3;
    private static String[] res = new String[3*count];
    private static int index = 0;
    private static int[] notifyarr = {1, 2, 3, 3, 2, 1, 2, 1, 3};

    public static void func1() throws InterruptedException {
        OpenThreeThread_three A = new OpenThreeThread_three() {
            public void run() {
                // for循环每个线程执行三遍
                for (int i = 0; i < count; i++) {
                    // 进入循环获取同步锁
                    synchronized (LOCK) {
                        // while循环，判断要执行的方法是不是本方法，执行序列在 notifyarr
                        while (index % 3 != 0) {
                            LOCK.notifyAll();   // 如果不是本方法通知其他线程继续执行
                            try {
                                LOCK.wait(); // 进入等待

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        res[index++] = "a";
                        LOCK.notifyAll();   // 通知其他线程
                    }
                }
            }
        };
        OpenThreeThread_three B = new OpenThreeThread_three() {
            public void run() {
                for (int i = 0; i < count; i++) {
                    synchronized (LOCK) {
                        while (index % 3 != 1) {
                            LOCK.notifyAll();
                            try {
                                LOCK.wait();

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        res[index++] = "l";
                        LOCK.notifyAll();
                    }
                }
            }
        };
        OpenThreeThread_three C = new OpenThreeThread_three() {
            public void run() {
                for (int i = 0; i < count; i++) {
                    synchronized (LOCK) {
                        while (index % 3 != 2) {
                            LOCK.notifyAll();
                            try {
                                LOCK.wait();

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        res[index++] = "i";
                        LOCK.notifyAll();
                    }
                }
            }
        };

        A.start();
        B.start();
        C.start();

        A.join();
        B.join();
        C.join();

        for (String str:res) {
            System.out.print(str);
        }
    }

    public static void func2() {
        OpenThreeThread_three A = new OpenThreeThread_three() {
            public void run() {
                // for循环每个线程执行三遍
                for (int i = 0; i < count; i++) {
                    // 进入循环获取同步锁
                    synchronized (LOCK) {
                        // while循环，判断要执行的方法是不是本方法，执行序列在 notifyarr
                        while (notifyarr[p] != 1) {
                            LOCK.notifyAll();   // 如果不是本方法通知其他线程继续执行
                            try {
                                LOCK.wait(); // 进入等待

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        System.out.print("a");
                        p++;    // 进入下一个要执行的方法
                        LOCK.notifyAll();   // 通知其他线程
                    }
                }
            }
        };
        OpenThreeThread_three B = new OpenThreeThread_three() {
            public void run() {
                for (int i = 0; i < count; i++) {
                    synchronized (LOCK) {
                        while (notifyarr[p] != 2) {
                            LOCK.notifyAll();
                            try {
                                LOCK.wait();

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        System.out.print("l");
                        p++;
                        LOCK.notifyAll();
                    }
                }
            }
        };
        OpenThreeThread_three C = new OpenThreeThread_three() {
            public void run() {
                for (int i = 0; i < count; i++) {
                    synchronized (LOCK) {
                        while (notifyarr[p] != 3) {
                            LOCK.notifyAll();
                            try {
                                LOCK.wait();

                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        System.out.print("i");
                        p++;
                        LOCK.notifyAll();
                    }
                }
            }
        };

        A.setName("A");
        B.setName("B");
        C.setName("C");

        /*
         * A.setPriority(4); B.setPriority(5); C.setPriority(3);
         */

        A.start();
        B.start();
        C.start();

        System.out.println("p=" + p);
    }

    public static void main(String[] args) throws InterruptedException {
        func1();

//        func2();
    }
}
