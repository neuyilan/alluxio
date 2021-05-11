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

import alluxio.client.file.FileInStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.ReadStatistics;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input stream for reading a file from HDFS. This is just a wrapper around {@link FileInStream}
 * with additional statistics gathering in a {@link Statistics} object.
 */
@NotThreadSafe
public class HdfsDataFileInputStream extends HdfsDataInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsDataFileInputStream.class);

  private HdfsFileInputStream mInputStream;
  private DFSInputStream mDfsInputStream;

  public HdfsDataFileInputStream(HdfsFileInputStream fileInStream, DFSInputStream dfsInputStream) throws IOException {
    super(dfsInputStream);
    mDfsInputStream = dfsInputStream;
    mInputStream = fileInStream;
  }

  @Override
  public int available() throws IOException {
    return mInputStream.available();
  }

  @Override
  public void close() throws IOException {
    mInputStream.close();
  }

  @Override
  public long getPos() throws IOException {
    return mInputStream.getPos();
  }

  @Override
  public int read() throws IOException {
    return mInputStream.read();
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return mInputStream.read(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    mInputStream.readFully(position, buffer);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    mInputStream.readFully(position, buffer, offset, length);
  }

  @Override
  public void seek(long pos) throws IOException {
    mInputStream.seek(pos);
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return mDfsInputStream.seekToNewSource(targetPos);
  }

  @Override
  public long skip(long n) throws IOException {
    return mInputStream.skip(n);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    LOG.debug("add by qihouliang, come in the read(ByteBuffer buf) in HdfsDataFileInputStream");
    return mInputStream.read(buf);
  }

  // TODO (qihouliang, only care about the total bytes read)
  // for 3.0 version and above
  @Override
  public ReadStatistics getReadStatistics() {
    ReadStatistics readStatistics = new ReadStatistics();
    readStatistics.addLocalBytes(mInputStream.getmStatistics().getBytesRead());
    LOG.info("add by qihouliang,{}, {}", mInputStream.getmStatistics(), readStatistics);
    return readStatistics;
  }

  /**
  // for 2.6.5 version
  public synchronized DFSInputStream.ReadStatistics getReadStatistics() {
    AlluxioReadStatistics alluxioReadStatistics = new AlluxioReadStatistics();
    alluxioReadStatistics.addLocalBytes(mInputStream.getmStatistics().getBytesRead());

    LOG.info("add by qihouliang111,{}, {}", mInputStream.getmStatistics().toString(), alluxioReadStatistics.toString());
    DFSInputStream.ReadStatistics parentResult = new DFSInputStream.ReadStatistics(alluxioReadStatistics);

    LOG.info("add by qihouliang parent,total {}", parentResult.getTotalBytesRead());
    LOG.info("add by qihouliang parent,getTotalLocalBytesRead {}", parentResult.getTotalLocalBytesRead());
    return parentResult;
  }

  class AlluxioReadStatistics extends DFSInputStream.ReadStatistics{
    private long totalBytesRead;
    private long totalLocalBytesRead;
    private long totalShortCircuitBytesRead;
    private long totalZeroCopyBytesRead;

    void addLocalBytes(long amt) {
      this.totalBytesRead += amt;
      this.totalLocalBytesRead += amt;
    }

    @Override
    public long getTotalBytesRead() {
      return totalBytesRead;
    }

    @Override
    public long getTotalLocalBytesRead() {
      return totalLocalBytesRead;
    }

    @Override
    public long getTotalShortCircuitBytesRead() {
      return totalShortCircuitBytesRead;
    }

    @Override
    public long getTotalZeroCopyBytesRead() {
      return totalZeroCopyBytesRead;
    }

    @Override
    public long getRemoteBytesRead() {
      return totalBytesRead - totalLocalBytesRead;
    }

    @Override
    public String toString() {
      return "ReadStatistics{"
          + " totalBytesRead=" + totalBytesRead
          + ", totalLocalBytesRead=" + totalLocalBytesRead
          + ", totalShortCircuitBytesRead=" + totalShortCircuitBytesRead
          + ", totalZeroCopyBytesRead=" + totalZeroCopyBytesRead
          + "}";
    }
  }
   **/

}
