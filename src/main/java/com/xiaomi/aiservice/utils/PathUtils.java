package com.xiaomi.aiservice.utils;

/**
 * @author wanglingda@xiaomi.com
 */

public class PathUtils {

  public static final String DWS_JOKE_COMMENT_REDUCED = "/user/h_misearch/ai/joke/data/joke_comment_reduced";

  public static String getCurrentPath(String path) {
    return path + "/current";
  }

  public static String getCurrentFile(String path) {
    return path + "/current/part-00000";
  }
}
