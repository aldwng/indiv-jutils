package com.xiaomi.aiservice.utils.pegasus;

import com.google.common.base.Throwables;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.common.perfcounter.PerfCounter;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusClientInterface;
import com.xiaomi.miliao.zookeeper.EnvironmentType;
import com.xiaomi.miliao.zookeeper.ZKFacade;

/**
 * @author wanglingda@xiaomi.com
 */

public class PegasusClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(PegasusClient.class);
  private static final String ZK_PREFIX = "zk://";
  private static final int EXPIRING_SECONDS = 21 * 24 * 3600;

  private PegasusClientInterface pInterface;

  public PegasusClient(EnvironmentType environmentType, String resourcePath) {
    String zkServer = ZKFacade.getZKSettings().getZKServers(environmentType);
    String zkConfigPath = ZK_PREFIX + zkServer + resourcePath;
    try {
      pInterface = PegasusClientFactory.getSingletonClient(zkConfigPath);
      LOGGER.info("created pegasus client successfully, env:{}, zkConfigPath:{}", environmentType, zkConfigPath);
    } catch (PException e) {
      LOGGER.info("creating pegasus client failed, env:{}, zkConfigPath:{}", environmentType, zkConfigPath);
      LOGGER.error("creating pegasus client failed due to {}", Throwables.getStackTraceAsString(e));
    }
  }

  public byte[] getDoc(String table, String key) {
    byte[] value = new byte[0];
    if (StringUtils.isBlank(key) || StringUtils.isBlank(table)) {
      return value;
    }
    long startTime = System.currentTimeMillis();
    byte[] keyBytes = key.getBytes();
    try {
      value = pInterface.get(table, keyBytes, null);
      if (value == null) {
        return new byte[0];
      }
    } catch (PException e) {
      LOGGER.error("pegasus:{} get doc key:{} failed due to {}", table, key,
                   Throwables.getStackTraceAsString(e));
    }
    long cost = System.currentTimeMillis() - startTime;
    PerfCounter.count("aiservice_get_pegasus_" + table, 1, cost);
    return value;
  }

  public void delDoc(String table, String key) {
    if (StringUtils.isBlank(key) || StringUtils.isBlank(table)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    try {
      byte[] keyBytes = key.getBytes();
      pInterface.del(table, keyBytes, null);
    } catch (PException e) {
      LOGGER.error("pegasus:{} del doc key:{} failed due to {}", table, key,
                   Throwables.getStackTraceAsString(e));
    }
    long cost = System.currentTimeMillis() - startTime;
    PerfCounter.count("aiservice_del_pegasus_" + table, 1, cost);
  }

  public void setDoc(String table, String key, String value) {
    if (StringUtils.isBlank(key) || StringUtils.isBlank(table) || StringUtils.isBlank(value)) {
      return;
    }
    long startTime = System.currentTimeMillis();
    try {
      byte[] keyBytes = key.getBytes();
      byte[] valueBytes = value.getBytes();
      pInterface.set(table, keyBytes, null, valueBytes, EXPIRING_SECONDS);
    } catch (PException e) {
      LOGGER.error("pegasus:{} set doc key:{} failed due to {}", table, key,
                   Throwables.getStackTraceAsString(e));
    }
    long cost = System.currentTimeMillis() - startTime;
    PerfCounter.count("aiservice_set_pegasus_" + table, 1, cost);
  }

  public void close() {
    pInterface.close();
  }
}