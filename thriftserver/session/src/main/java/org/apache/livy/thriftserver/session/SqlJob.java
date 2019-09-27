/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.thriftserver.session;

import java.util.Iterator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import org.apache.livy.Job;
import org.apache.livy.JobContext;

/**
 * A Job implementation for executing SQL queries in a Livy session.
 */
public class SqlJob implements Job<Void> {

  private final String sessionId;
  private final String statementId;
  private final String statement;
  private final String defaultIncrementalCollect;
  private final String incrementalCollectEnabledProp;

  public SqlJob() {
    this(null, null, null, null, null);
  }

  public SqlJob(
      String sessionId,
      String statementId,
      String statement,
      String defaultIncrementalCollect,
      String incrementalCollectEnabledProp) {
    this.sessionId = sessionId;
    this.statementId = statementId;
    this.statement = statement;
    this.defaultIncrementalCollect = defaultIncrementalCollect;
    this.incrementalCollectEnabledProp = incrementalCollectEnabledProp;
  }

  @Override
  public Void call(JobContext ctx) throws Exception {
    ctx.sc().setJobGroup(statementId, statement);
    try {
      executeSql(ctx);
    } finally {
      ctx.sc().clearJobGroup();
    }
    return null;
  }

  private void executeSql(JobContext ctx) throws Exception {
    ThriftSessionState session = ThriftSessionState.get(ctx, sessionId);
    SparkSession spark = session.spark();
    Dataset<Row> df = spark.sql(statement);
    StructType schema = df.schema();

    boolean incremental = Boolean.parseBoolean(
        spark.conf().get(incrementalCollectEnabledProp, defaultIncrementalCollect));

    Iterator<Row> iter = new LazyIterator<>(df, incremental);

    // Register both the schema and the iterator with the session state after the statement
    // has been executed.
    session.registerStatement(statementId, schema, iter);
  }

  private static class LazyIterator<T> implements Iterator<T> {
    private final Dataset<T> df;
    private final boolean incremental;
    private volatile Iterator<T> it;

    LazyIterator(Dataset<T> df, boolean incremental) {
      this.df = df;
      this.incremental = incremental;
    }

    @Override
    public boolean hasNext() { return getIterator().hasNext(); }

    @Override
    public T next() {
      return getIterator().next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    private Iterator<T> getIterator() {
      if (it == null) {
        synchronized (this) {
          if (it == null) {
            it = incremental ? new ScalaIterator<>(df.rdd().toLocalIterator()) :
              df.collectAsList().iterator();
          }
        }
      }
      return it;
    }
  }
}
