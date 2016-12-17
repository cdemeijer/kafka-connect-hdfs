/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.StorageSinkTestBase;

public class HdfsSinkConnectorTestBase extends StorageSinkTestBase {

  protected Configuration conf;
  protected HdfsSinkConnectorConfig connectorConfig;
  protected String topicsDir;
  protected String logsDir;
  protected AvroData avroData;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = new HashMap<>();
    props.put(HdfsSinkConnectorConfig.HDFS_URL_CONFIG, url);
    props.put(HdfsSinkConnectorConfig.FLUSH_SIZE_CONFIG, "3");
    return props;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    conf = new Configuration();
    url = "memory://";
    super.setUp();
    // Configure immediately in setup for common case of just using this default. Subclasses can
    // re-call this safely.
    configureConnector();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    if (assignment != null) {
      assignment.clear();
    }
  }

  protected void configureConnector() {
    connectorConfig = new HdfsSinkConnectorConfig(properties);
    topicsDir = connectorConfig.getString(HdfsSinkConnectorConfig.TOPICS_DIR_CONFIG);
    logsDir = connectorConfig.getString(HdfsSinkConnectorConfig.LOGS_DIR_CONFIG);
    int schemaCacheSize = connectorConfig.getInt(HdfsSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
    avroData = new AvroData(schemaCacheSize);
  }
}
