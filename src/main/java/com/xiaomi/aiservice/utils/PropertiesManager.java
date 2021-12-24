package com.xiaomi.aiservice.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.miliao.common.DataUpdateListener;

public class PropertiesManager implements DataUpdateListener<Properties> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesManager.class);
  private static final String SEARCH_PROPERTIES = "/search.properties";
  private static final String ZOOKEEPER_PROPERTIES = "/zookeeper.properties";
  private static final Charset C_S_UTF_8 = StandardCharsets.UTF_8;
  private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(true);
  private final ConcurrentLinkedQueue<Listener> listeners;
  private Properties properties;

  private static final PropertiesManager INSTANCE = new PropertiesManager();

  private PropertiesManager() {
    this.listeners = new ConcurrentLinkedQueue<>();
    properties = new Properties();
    try {
      InputStream zkStream = PropertiesManager.class.getResourceAsStream(ZOOKEEPER_PROPERTIES);
      InputStream schStream = PropertiesManager.class.getResourceAsStream(SEARCH_PROPERTIES);
      if (zkStream != null) {
        properties.load(new InputStreamReader(zkStream, C_S_UTF_8));
      }
      if (schStream != null) {
        properties.load(new InputStreamReader(schStream, C_S_UTF_8));
      }
    } catch (IOException e) {
      LOGGER.error("load properties failed due to {}", Throwables.getStackTraceAsString(e));
    }
  }

  public static PropertiesManager getInstance() {
    return INSTANCE;
  }

  public void addListener(Listener listener) {
    this.listeners.add(listener);
  }

  public Properties getProperties() {
    ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
    if (readLock.tryLock()) {
      try {
        return properties;
      } finally {
        readLock.unlock();
      }
    }
    return null;
  }

  public String getProperty(String name) {
    ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
    if (readLock.tryLock()) {
      try {
        return properties.getProperty(name);
      } finally {
        readLock.unlock();
      }
    }
    return null;
  }

  public String getProperty(String name, String defaultValue) {
    ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
    if (readLock.tryLock()) {
      try {
        return properties.getProperty(name, defaultValue);
      } finally {
        readLock.unlock();
      }
    }
    return null;
  }

  @Override
  public void onUpdated(Properties updatedData) {
    ReentrantReadWriteLock.WriteLock writeLock = reentrantReadWriteLock.writeLock();
    writeLock.lock();
    try {
      Properties merged = new Properties();
      merged.putAll(properties);
      merged.putAll(updatedData);
      properties = merged;
      publishListeners();
    } finally {
      writeLock.unlock();
    }
  }

  private void publishListeners() {
    if (listeners != null) {
      for (Listener listener : listeners) {
        listener.notice(properties);
      }
    }
  }

  public interface Listener {

    void notice(Properties property);
  }
}
