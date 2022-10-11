/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests {@link LeetCode}.
 */
public class LeetCode {

  @Test
  public void testActiveRecords() throws IOException {
    int res = maxProfit(new int[] {1, 3, 2, 8, 4, 9}, 2);
    System.out.println(res);
  }

  public int maxProfit(int[] prices, int fee) {
    int lastPrice = -1;
    int profit = 0;
    int max = 0;
    for (int i = 0; i < prices.length; i++) {
      if (lastPrice != -1) {
        System.out.println(lastPrice + "-" + i + "-" + max);
        for (int j = i; j < prices.length; j++) {
          System.out.println(lastPrice + "-" + j + "-" + max + "-" + prices[j] + "--" + Math.max(max, fee));
          if ((prices[j] - lastPrice) >  Math.max(max, fee)) {
            max = prices[j] - lastPrice;
            System.out.println("111");

          } else if ((lastPrice + max - prices[j]) > fee) {
            profit = profit + max - fee;
            lastPrice = prices[j];
            max = 0;
            break;
          }
        }
      } else {
        for (int j = i + 1; j < prices.length; j++) {
          if (prices[i] > prices[j]) {
            break;
          } else if (prices[j] - prices[i] > fee) {
            max = prices[j] - prices[i];
            lastPrice = prices[i];
            i = j;
          }
        }
      }
    }
    return profit + max -fee;
  }

  public boolean areAlmostEqual(String s1, String s2) {
    int first = -1;
    int second = -1;
    for(int i=0;i<s1.length();i++){
     if(s1.charAt(i)!=s2.charAt(i)){
       if(first==-1){
         first = i;
       }
       else if(second==-1){
         second =i;
       }
       else {
         return false;
       }
     }
    }
    if(first!=-1 && second!=-1){
      return s1.charAt(first)==s2.charAt(second) && s1.charAt(second)==s2.charAt(first);
    }
    return false;
  }
  public class TreeNode {
      int val;
      TreeNode left;
      TreeNode right;
      TreeNode() {}
      TreeNode(int val) { this.val = val; }
      TreeNode(int val, TreeNode left, TreeNode right) {
          this.val = val;
          this.left = left;
          this.right = right;
      }
  }

  private List<Integer> ans = new ArrayList<>();

  public List<Integer> preorderTraversal(TreeNode root) {
    if (root != null) {
      ans.add(root.val);
      preorderTraversal(root.left);
      preorderTraversal(root.right);
    }

    return ans;
  }

  public String compressString(String S) {
    StringBuilder sb = new StringBuilder();
    int count = 1;
    for (int i = 1; i < S.length(); i++) {
      if (S.charAt(i) == S.charAt(i - 1)) {
        count++;
      } else {
        sb.append(String.valueOf(S.charAt(i - 1)) + count);
        count = 1;
      }
    }
    return sb.toString().length() >= S.length() ? S : sb.toString();
  }

  //R3G2B1
  public int countPoints(String rings) {
    int[] sticks = new int[10];
    for(int i=0;i<rings.length();i++){
      if(i%2==0){
        int stick = Character.getNumericValue(rings.charAt(i+1));
        if(rings.charAt(i)=='R') {
          sticks[stick] = sticks[stick]|1;
        }
        else if(rings.charAt(i)=='G'){
          sticks[stick] = sticks[stick]|2;
        }
        else {
          sticks[stick] = sticks[stick]|4;
        }
      }
    }
    int ans = 0;
    for(int i=0;i< sticks.length;i++){
     if(sticks[i]==7)ans++;
    }
    return ans;
  }

  public int lengthOfLIS(int[] nums) {
    int dp[] = new int[nums.length];
    dp[0]=1;
    for(int i=1;i<nums.length;i++){
      dp[i] = 1;
      int j = i-1;
      while (nums[j] >= nums[i] && j >= 0) {
        j--;
      }
      if(nums[j]<nums[i]){
        dp[i]=dp[i]+dp[j];
      }
    }
    return Arrays.stream(dp).max().orElse(0);
  }
  public class ListNode {
      int val;
      ListNode next;
      ListNode() {}
      ListNode(int val) { this.val = val; }
      ListNode(int val, ListNode next) { this.val = val; this.next = next; }
  }

  public ListNode addTwoNumbers(ListNode l1, ListNode l2) {
    boolean addOne = false;
    ListNode root1 = l1;
    ListNode root2 = l2;

    if (l1.val + l2.val > 10) {
      addOne = true;
    }
    l1.val = (l1.val + l2.val) % 10;
    while (l1.next != null && l2.next != null) {
      l1 = l1.next;
      l2 = l2.next;
      l1.val = (l1.val + l2.val) % 10 + (addOne ? 1 : 0);
      l2.val = (l1.val + l2.val) % 10 + (addOne ? 1 : 0);
      addOne = false;
      if (l1.val + l2.val >= 10) {
        addOne = true;
      }
    }
    if (l1.next != null) {
      if (addOne) {
        l1.next.val = l1.next.val + 1;
      }
      return root1;
    }
    if (l2.next != null) {
      if (addOne) {
        l2.next.val = l2.next.val + 1;
      }
      return root2;
    }
    return root1;
  }
}
