/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricsConfig;
import alluxio.metrics.MetricsSystem;

import com.google.common.base.Preconditions;
import java.io.InputStream;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSHedgedReadMetrics;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * An Alluxio client compatible with Apache Hadoop {@link org.apache.hadoop.fs.FileSystem}
 * interface, using Alluxio local cache. This client will first consult the local cache before
 * requesting the remote Hadoop FileSystem in case of cache misses.
 */
public class LocalCacheFileSystem extends org.apache.hadoop.hdfs.DistributedFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCacheFileSystem.class);
  private static final Set<String> SUPPORTED_FS = new HashSet<String>() {
    {
      add(Constants.SCHEME);
      add("ws");
      add("hdfs");
    }
  };

  /**
   * The external Hadoop filesystem to query on cache miss.
   */
  private org.apache.hadoop.fs.FileSystem mExternalFileSystem;
  private HadoopFileOpener mHadoopFileOpener;
  private LocalCacheFileInStream.FileInStreamOpener mAlluxioFileOpener;
  private CacheManager mCacheManager;
  private org.apache.hadoop.conf.Configuration mHadoopConf;
  private AlluxioConfiguration mAlluxioConf;
  private DFSInputStream dfsInputStream;

  public LocalCacheFileSystem() {
  }

  /**
   * @param fileSystem File System instance
   */
  public LocalCacheFileSystem(org.apache.hadoop.fs.FileSystem fileSystem) {
    this(fileSystem, uriStatus -> fileSystem.open(new Path(uriStatus.getPath())));
  }

  /**
   * @param fileSystem File System instance
   * @param fileOpener File opener instance
   */
  public LocalCacheFileSystem(org.apache.hadoop.fs.FileSystem fileSystem,
      HadoopFileOpener fileOpener) {
    mExternalFileSystem = Preconditions.checkNotNull(fileSystem, "filesystem");
    mHadoopFileOpener = Preconditions.checkNotNull(fileOpener, "fileOpener");
    mAlluxioFileOpener = status -> new AlluxioHdfsInputStream(mHadoopFileOpener.open(status));
  }

  @Override
  public synchronized void initialize(URI uri, org.apache.hadoop.conf.Configuration conf)
      throws IOException {
    if (!SUPPORTED_FS.contains(uri.getScheme())) {
      throw new UnsupportedOperationException(
          uri.getScheme() + " is not supported as the external filesystem.");
    }

    mHadoopConf = conf;
    // Set statistics
    setConf(conf);

    String hdfsImpl = "fs.hdfs.impl";
    String origin = getConf().get(hdfsImpl);
    getConf().set(hdfsImpl, DistributedFileSystem.class.getName());
    try {
      org.apache.hadoop.fs.FileSystem fileSystem = org.apache.hadoop.fs.FileSystem
          .get(uri, getConf());
      HadoopFileOpener fileOpener = uriStatus -> fileSystem.open(new Path(uriStatus.getPath()));
      mExternalFileSystem = Preconditions.checkNotNull(fileSystem, "filesystem");
      mHadoopFileOpener = Preconditions.checkNotNull(fileOpener, "fileOpener");
      mAlluxioFileOpener = status -> new AlluxioHdfsInputStream(mHadoopFileOpener.open(status));
    } catch (IOException e) {
      LOG.error("initialize LocalCacheFileSystem failed");
    }

    getConf().set(hdfsImpl, origin);

    super.initialize(uri, conf);
    mAlluxioConf = HadoopUtils.toAlluxioConf(mHadoopConf);
    // Handle metrics
    Properties metricsProperties = new Properties();
    for (Map.Entry<String, String> entry : conf) {
      metricsProperties.setProperty(entry.getKey(), entry.getValue());
    }
    MetricsSystem.startSinksFromConfig(new MetricsConfig(metricsProperties));
    mCacheManager = CacheManager.Factory.get(mAlluxioConf);
    LOG.debug("add qihouliang, initialize the LocalCacheFileSystem, mCacheManager={}",
        mCacheManager);
  }

  @Override
  public void close() throws IOException {
    // super.close should be called first before releasing the resources in this instance, as the
    // super class may invoke other methods in this class. For example,
    // org.apache.hadoop.fs.FileSystem.close may check the existence of certain temp files before
    // closing
    super.close();
  }

  /**
   * @return scheme
   */
  // @Override This doesn't exist in Hadoop 1.x, so cannot put {@literal @Override}.
  public String getScheme() {
    return mExternalFileSystem.getScheme();
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    LOG.debug("qihouliang, open 111111111");
    if (mCacheManager == null) {
      return mExternalFileSystem.open(path, bufferSize);
    }
    URIStatus status = HadoopUtils.toAlluxioUriStatus(mExternalFileSystem.getFileStatus(path));
    LOG.debug("qihouliang, open 22222222");
    return open(status, bufferSize);
  }

  /**
   * Attempts to open the specified file for reading.
   *
   * @param status     the status of the file to open
   * @param bufferSize stream buffer size in bytes, currently unused
   * @return an {@link FSDataInputStream} at the indicated path of a file
   */
  public FSDataInputStream open(URIStatus status, int bufferSize) throws IOException {
    if (mCacheManager == null) {
      return mExternalFileSystem.open(HadoopUtils.toPath(new AlluxioURI(status.getPath())),
          bufferSize);
    }

    LocalCacheFileInStream localCacheFileInStream =
        new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf);
    localCacheFileInStream.init();

    AlluxioHdfsInputStream alluxioHdfsInputStream = (AlluxioHdfsInputStream) localCacheFileInStream
        .getmExternalFileInStream();
    FSDataInputStream fsDataInputStream = alluxioHdfsInputStream.getmInput();

    InputStream inputStream = fsDataInputStream.getWrappedStream();
    dfsInputStream = getDFSInputStream(inputStream);

    /* the original codes
    return new FSDataInputStream(new HdfsFileInputStream(
        new LocalCacheFileInStream(status, mAlluxioFileOpener, mCacheManager, mAlluxioConf),
        statistics));
     **/

    return new HdfsDataFileInputStream(new HdfsFileInputStream(localCacheFileInStream, statistics),
        dfsInputStream);
  }

  @Override
  public URI getUri() {
    return mExternalFileSystem.getUri();
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return mExternalFileSystem.create(f, permission, overwrite, bufferSize, replication, blockSize,
        progress);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return mExternalFileSystem.append(f, bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return mExternalFileSystem.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return mExternalFileSystem.delete(f, recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return mExternalFileSystem.listStatus(f);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    mExternalFileSystem.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getWorkingDirectory() {
    return mExternalFileSystem.getWorkingDirectory();
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return mExternalFileSystem.mkdirs(f, permission);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return mExternalFileSystem.getFileStatus(f);
  }

  @Override
  public boolean isFileClosed(final Path src) throws IOException {
    return ((DistributedFileSystem) mExternalFileSystem).isFileClosed(src);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    return mExternalFileSystem.getContentSummary(f);
  }

  @Override
  public boolean recoverLease(final Path f) throws IOException {
    return ((DistributedFileSystem) mExternalFileSystem).recoverLease(f);
  }

  @Override
  public DFSHedgedReadMetrics getHedgedReadMetrics() {
    return ((DistributedFileSystem) mExternalFileSystem).getHedgedReadMetrics();
  }

  private DFSInputStream getDFSInputStream(InputStream in) throws IOException{
    if (in instanceof CryptoInputStream) {
      return (DFSInputStream) ((CryptoInputStream) in).getWrappedStream();
    } else if (in instanceof DFSInputStream) {
      return (DFSInputStream) in;
    }
    throw new IOException("getDFSInputStream failed, in="+ in.toString());
  }
}
