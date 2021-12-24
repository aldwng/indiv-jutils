package com.xiaomi.aiservice.utils;

/**
 * @author wanglingda@xiaomi.com
 */

public class PathUtils {

  public static String getCurrentPath(String path) {
    return path + "/current";
  }

  public static String getCurrentFile(String path) {
    return path + "/current/part-00000";
  }
}
