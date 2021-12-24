package com.xiaomi.aiservice.utils.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by mi on 18-3-8.
 */
public class HdfsResourceLoader {

  private final FileSystem fileSystem;

  public HdfsResourceLoader(HdfsEnv hdfsEnv) throws IOException {
    Configuration configuration = new Configuration(false);
    switch (hdfsEnv) {
      case ZJY:
        configuration.addResource("core-site_zjy.xml");
        configuration.addResource("hdfs-site_zjy.xml");
        break;
      case STAGING_TJ:
        configuration.addResource("core-site_staging_tj.xml");
        configuration.addResource("hdfs-site_staging_tj.xml");
        break;
    }
    fileSystem = FileSystem.get(configuration);
  }

  public void loadInDirectory(String hdfsPath, HdfsLineHandler lineHandler) throws IOException {
    FileStatus[] fileStatuses = fileSystem.listStatus(new Path(hdfsPath));
    for (FileStatus fileStatus : fileStatuses) {
      try (BufferedReader br = new BufferedReader(
          new InputStreamReader(fileSystem.open(fileStatus.getPath())))) {
        readBufferedData(br, lineHandler);
      }
    }
  }

  public void loadFile(String hdfsPath, HdfsLineHandler lineHandler) throws IOException {
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(fileSystem.open(new Path(hdfsPath))))) {
      readBufferedData(br, lineHandler);
    }
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  private void readBufferedData(BufferedReader bufferedReader, HdfsLineHandler lineHandler) throws IOException {
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      lineHandler.handle(line);
    }
  }
}
