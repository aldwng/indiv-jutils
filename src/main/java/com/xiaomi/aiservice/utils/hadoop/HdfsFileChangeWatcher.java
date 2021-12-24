package com.xiaomi.aiservice.utils.hadoop;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.base.Function;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsFileChangeWatcher extends Thread implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileChangeWatcher.class);
  private final Function<String, ?> callback;
  private final String absolutePath;
  private final int interval;
  private final HdfsResourceLoader hdfsLoader;
  private long lastWatch;

  public HdfsFileChangeWatcher(String absolutePath, HdfsResourceLoader hdfsLoader, Function<String, ?> callback) {
    this(absolutePath, callback, 3 * 60 * 1000, hdfsLoader);
  }

  public HdfsFileChangeWatcher(String absolutePath, Function<String, ?> callback, int interval,
                               HdfsResourceLoader hdfsLoader) {
    this.absolutePath = absolutePath;
    this.callback = callback;
    this.interval = interval;
    this.hdfsLoader = hdfsLoader;
    this.lastWatch = 0;
    this.setDaemon(true);
  }

  @Override
  public void run() {
    try {
      LOGGER.info("hdfs file watcher start to watch {}", absolutePath);
      while (true) {
        try {
          if (hdfsLoader.getFileSystem().exists(new Path(absolutePath))) {
            FileStatus fileStatus = hdfsLoader.getFileSystem().getFileStatus(new Path(absolutePath));
            LOGGER.info("hdfs file watcher {}, lastChange {}", fileStatus, lastWatch);
            if (fileStatus.getModificationTime() > lastWatch) {
              doOnChange();
              lastWatch = System.currentTimeMillis();
            } else {
              LOGGER.info("hdfs file watcher find no modification on {}", absolutePath);
            }
          } else {
            LOGGER.warn("hdfs file watcher not exist {}", absolutePath);
          }
        } catch (Exception e) {
          LOGGER.error("hdfs file watcher thread error {} {}", absolutePath, e);
        }
        Thread.sleep(interval);
      }
    } catch (Exception e) {
      LOGGER.error("hdfs file watch error", e);
    }
  }

  public void doOnChange() {
    this.callback.apply(absolutePath);
  }

  @Override
  public void close() throws IOException {
    this.interrupt();
  }
}
