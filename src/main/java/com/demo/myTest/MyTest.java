package com.demo.myTest;

public class MyTest {
    /**
     * LeetCode - 72    编辑距离
     * 给定两个单词 word1 和 word2，计算出将 word1 转换成 word2 所使用的最少操作数 。你可以对一个单词进行如下三种操作：
     * 插入一个字符
     * 删除一个字符
     * 替换一个字符
     *
     * 输入: word1 = "horse", word2 = "ros"
     * 输出: 3
     * 解释:
     * horse -> rorse (将 'h' 替换为 'r')
     * rorse -> rose (删除 'r')
     * rose -> ros (删除 'e')
     *
     * @param word1
     * @param word2
     * @return
     */
    public static int fun(String word1, String word2) {
        int m = word1.length();
        int n = word2.length();
        if (m < 0 || n <0)
            return 0;
        int[][] dp = new int[m][n];
        //设置初始值
        if (word1.charAt(0) == word2.charAt(0))
            dp[0][0] = 0;
        else
            dp[0][0] = 1;
        //设置第一列的初始值
        for (int i = 1; i < m; i++)
            dp[i][0] = dp[i-1][0] + 1;
        //设置第一行的初始值
        for (int j = 1; j < n; j++)
            dp[0][j] = dp[0][j-1] + 1;

        //根据状态转换方程计算dp[m-1][n-1]
        /**
         * 1. 新增的字符word[i]和word[j]相同：dp[i][j]基于word1的前i-1个字符和word2的前j-1个字符处理好的情况下，新增的字符相等，不需要进行操作。即 dp[i][j] = dp[i-1][j-1]， 比如：word1="ab", word2="ab"
         * 2. 新增的字符不相等：
         * 	1. 需要替换：dp[i][j]基于word1的前i-1个字符和word2的前j-1个字符处理好的情况下，新增的字符相等，需要进行替换操作，则 dp[i][j] = dp[i-1][j-1] + 1， 比如：word1="abc", word2="abb"
         * 	2. 需要删除：不需要多的word[i]字符，dp[i][j]基于word1的前i-1个字符和word2的前j个字符处理好的情况下，删除word[i]，则dp[i][j] = dp[i-1][j] + 1， 比如：word1="abbb", word2="abb"
         * 	3. 需要插入：需要在word[i+1]处添加字符，但是dp[i][j]的状态是基于word1的前i个字符和word2的前j-1个字符处理好的情况下，则dp[i][j] = dp[i][j-1] + 1， 比如：word1="ab", word2="abb"
         */
        for (int i = 1; i < m; i++) {
            for (int j = 1; j < n; j++) {
                if (word1.charAt(i) == word2.charAt(j)) dp[i][j] = dp[i-1][j-1];
                else dp[i][j] = 1 + Math.min(Math.min(dp[i-1][j], dp[i][j-1]) , dp[i-1][j-1]);
            }

        }
        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                System.out.print(dp[i][j] + " ");
            }
            System.out.println();
        }
        return dp[m-1][n-1];
    }

    public static void main(String[] args) {
        String word1 = "horse", word2 = "ros";
        System.out.println(MyTest.fun(word1, word2));
    }
}

