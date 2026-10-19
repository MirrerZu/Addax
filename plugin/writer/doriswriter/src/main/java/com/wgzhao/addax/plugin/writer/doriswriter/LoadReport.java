/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.wgzhao.addax.plugin.writer.doriswriter;

import com.alibaba.fastjson2.JSON;
import com.wgzhao.addax.core.exception.AddaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.wgzhao.addax.core.spi.ErrorCode.CONFIG_ERROR;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Load Report: one JSON line per stream load attempt, appended to the file named by loadReport.
 *
 * <p>A "begin" line is written before the request is sent and an "end" line with Doris' response
 * or the error once the attempt is over; a batch dropped after a failure gets a "discard" line.
 * A caller that stops the job, or loses the process, can thus tell which labels may have reached
 * Doris and look their state up, instead of parsing the log.  Without loadReport nothing is
 * written.</p>
 */
public class LoadReport
        implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(LoadReport.class);

    private final FileChannel channel;
    private final int taskId;

    private LoadReport(FileChannel channel, int taskId)
    {
        this.channel = channel;
        this.taskId = taskId;
    }

    /** Opens the report of one writer task; tasks of a job may share the file. */
    public static LoadReport open(DorisKey options, int taskId)
    {
        String path = options.getLoadReport();
        if (path == null) {
            return new LoadReport(null, taskId);
        }
        try {
            return new LoadReport(FileChannel.open(Path.of(path), CREATE, WRITE, APPEND), taskId);
        }
        catch (IOException e) {
            throw AddaxException.asAddaxException(CONFIG_ERROR, "Cannot open the load report " + path, e);
        }
    }

    /** Must succeed before the request is sent: a load the report does not know of would be lost to the caller. */
    public void begin(WriterTuple batch, int attempt)
            throws IOException
    {
        Map<String, Object> line = line("begin", batch);
        line.put("attempt", attempt);
        line.put("rows", batch.getRows().size());
        line.put("bytes", batch.getBytes());
        write(line);
    }

    /** Records the outcome of an attempt, with the response when Doris gave one. */
    public void end(WriterTuple batch, int attempt, Throwable error)
            throws IOException
    {
        Map<String, Object> line = line("end", batch);
        line.put("attempt", attempt);
        line.put("success", error == null);
        line.put("response", batch.getResponse());
        if (error != null) {
            line.put("error", String.valueOf(error.getMessage()));
        }
        write(line);
    }

    /** Records a batch that is never sent; failing to do so must not hide the failure that caused it. */
    public void discard(WriterTuple batch)
    {
        Map<String, Object> line = line("discard", batch);
        line.put("rows", batch.getRows().size());
        try {
            write(line);
        }
        catch (IOException e) {
            LOG.warn("Failed to write to the load report.", e);
        }
    }

    private Map<String, Object> line(String event, WriterTuple batch)
    {
        Map<String, Object> line = new LinkedHashMap<>();
        line.put("event", event);
        line.put("time", Instant.now().toString());
        line.put("task", taskId);
        line.put("label", batch.getLabel());
        return line;
    }

    private synchronized void write(Map<String, Object> line)
            throws IOException
    {
        if (channel == null) {
            return;
        }
        // one write per line: tasks sharing the file append whole lines
        ByteBuffer buffer = ByteBuffer.wrap((JSON.toJSONString(line) + "\n").getBytes(StandardCharsets.UTF_8));
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (channel != null) {
            channel.close();
        }
    }
}
