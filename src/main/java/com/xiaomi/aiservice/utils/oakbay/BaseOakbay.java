package com.xiaomi.aiservice.utils.oakbay;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.oakbay.java.client.OakbayClient;
import com.xiaomi.oakbay.thrift.OakbaySearchRequest;
import com.xiaomi.oakbay.thrift.OakbaySearchResult;
import com.xiaomi.oakbay.thrift.OakbayTemplateBqlRequest;

/**
 * @author wanglingda@xiaomi.com
 */

public class BaseOakbay {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseOakbay.class);

  protected String domain;
  protected String cluster;
  protected int timeOut;
  protected OakbayClient client;

  private BaseOakbay() {
  }

  public BaseOakbay(String env, String domain, String cluster, int timeOut) {
    this.domain = domain;
    this.cluster = cluster;
    this.timeOut = timeOut;
    client = new OakbayClient(OakbayClient.Env.valueOf(env), timeOut);
    LOGGER.info("oakbay client init, domain:{}, cluster:{}, timeOut:{}, env:{}",
                this.domain, this.cluster, this.timeOut, env);
  }

  public OakbaySearchResult search(String service, String schJson) {
    long startT = System.currentTimeMillis();
    OakbayTemplateBqlRequest request = new OakbayTemplateBqlRequest();
    request.setDomain(domain);
    request.setCluster(cluster);
    request.setName(service);
    request.setQuery(schJson);
    OakbaySearchResult result;
    try {
      result = client.searchTemplateBQL(request);
    } catch (Exception e) {
      LOGGER.error("search {} {} {} {} failed due to {}", domain, cluster, service, schJson,
                   Throwables.getStackTraceAsString(e));
      return null;
    }
    if (!result.success) {
      LOGGER.error("search {} {} {} {} error:{}", domain, cluster, service, schJson, result.error);
      return null;
    }
    long cost = System.currentTimeMillis() - startT;
    LOGGER.info("domain:{} service:{} query:{} hits:{} cost:{}", domain, service, schJson,
                result.lindenResult.totalHits, cost);
    //Add perfCounter
    return result;
  }

  public OakbaySearchResult search(String bql) {
    long startT = System.currentTimeMillis();
    OakbaySearchRequest request = new OakbaySearchRequest(domain, cluster);
    request.setBql(bql);
    try {
      OakbaySearchResult result = client.search(request);
      long cost = System.currentTimeMillis() - startT;
      if (result.success) {
        LOGGER.info("domain:{} bql:{} hits:{} cost:{}", domain, bql, result.lindenResult.totalHits, cost);
        return result;
      }
    } catch (Exception e) {
      LOGGER.error("search bql:{} failed due to {}", bql, Throwables.getStackTraceAsString(e));
    }
    LOGGER.info("search bql:{} failed", bql);
    return null;
  }
}
