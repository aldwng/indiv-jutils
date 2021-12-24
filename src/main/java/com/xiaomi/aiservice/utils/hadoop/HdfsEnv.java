package com.xiaomi.aiservice.utils.hadoop;

/**
 * Created by mi on 18-3-8.
 */
public enum HdfsEnv {
  C3("c3"), STAGING_TJ("staging_tj"), ZJY("zjy"), ALSGP("alsgp");

  private final String name;

  HdfsEnv(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
