package com.savy3.foop;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.hadoop.fs.ftp;



import java.io.IOException;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
/**
 * <p>
 * A {@link FileSystem} backed by an FTP client provided by <a
 * href="http://commons.apache.org/net/">Apache Commons Net</a>.
 * </p>
 */
public class FTPBinaryRDWFileSystem extends FTPFileSystem {

  /**
   * Connect to the FTP server using configuration parameters *
   * 
   * @return An FTPClient instance
   * @throws IOException
   */
  private FTPClient connect() throws IOException {
    FTPClient client = null;
    Configuration conf = getConf();
    String host = conf.get("fs.ftp.host");
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    String user = conf.get("fs.ftp.user." + host);
    String password = conf.get("fs.ftp.password." + host);
    client = new FTPClient();
    System.out.println("Connecting to :"+host+":"+port);
    client.connect(host, port);
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new IOException("Server - " + host
          + " refused connection on port - " + port);
    } else if (client.login(user, password)) {
      client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE);
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.setBufferSize(DEFAULT_BUFFER_SIZE);
      client.featureValue("LITERAL SITE RDW");
      client.doCommand("SITE", "RDW");
    } else {
      throw new IOException("Login failed on server - " + host + ", port - "
          + port);
    }

    return client;
  }

  
}