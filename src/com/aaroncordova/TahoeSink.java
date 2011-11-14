/*
 * Copyright 2011 Aaron Cordova
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.aaroncordova;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.CustomDfsSink;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.OutputFormat;


public class TahoeSink extends EventSink.Base {

	final static Logger LOG = Logger.getLogger(CustomDfsSink.class.getName());
	
	private String path;
	private OutputFormat format;
	private OutputStream writer;
	private HashMap<String,OutputStream> writers;
	private boolean subbing;
	
	public TahoeSink(String path, OutputFormat format) {

		this.path = path;
		this.format = format;
		this.writer = null;
		this.writers = new HashMap<String,OutputStream>();
		this.subbing = Event.containsTag(path);
		
	}

	public void open() throws IOException {
		if(!subbing)
			writer = open(path);
	}
	
	private OutputStream open(String path) throws IOException {
		FlumeConfiguration conf = FlumeConfiguration.get();

		String rootCap = conf.get("flume.tahoe.rootcap");
		if(rootCap == null) 
			throw new IOException("error: please set tahoe.rootcap in configuration");

		// TODO url-encode file names. directory names too?
		// TODO get mutable flag from FsPermission object
		String req = "http://localhost:3456/uri/" + rootCap + path;
		//String req = "http://localhost:3456/uri/" + path;
		URL url = new URL(req);
		//System.out.println(req);

		// Open the connection and prepare to POST
		HttpURLConnection uc = (HttpURLConnection) url.openConnection();
		uc.setDoOutput(true);
		uc.setRequestMethod("PUT");
		uc.setChunkedStreamingMode(64000000); // 64mb chunks 

		// TODO: check whether the file exists
		return new LAFSOutputStream(uc);
	}

	public void append(Event e) throws IOException {
		//if(writer == null)
		//	open();
		OutputStream w = writer;
		
		if(subbing) {
			String writerPath = e.escapeString(path);
			w = writers.get(writerPath);
			if(w == null) {
				w = open(writerPath);
				writers.put(writerPath, w);
			}	
		}
		
		format.format(w, e);
		
		// for book keeping
		//super.append(e);
	}

	public void close() throws IOException {
		
		if(writer != null) {
			writer.close();
			writer = null;
		}
		
		// try to close them all
		// throw one of the exceptions if there are any
		// likely we'll have all or none succeed
		IOException exception = null;
		for(OutputStream w : writers.values()) {
			try {
				w.close();
			}
			catch(IOException e) {
				exception = e;
			}
		}
		
		if(exception != null)
			throw exception;
	}

	public String getName() {
		return "TahoeLAFS";
	}


	public static SinkBuilder builder() {
	    return new SinkBuilder() {
	      @Override
	      public EventSink build(Context context, String... args) {
	        if (args.length != 2 && args.length != 1) {
	          // TODO (jon) make this message easier.
	          throw new IllegalArgumentException(
	              "usage: tahoe(\"[path\", \"format\")");
	        }

	        String format = (args.length == 1) ? null : args[1];
	        OutputFormat fmt;
	        try {
	          fmt = FormatFactory.get().getOutputFormat(format);
	        } catch (FlumeSpecException e) {
	          LOG.error("failed to load format " + format, e);
	          throw new IllegalArgumentException("failed to load format " + format);
	        }
	        return new TahoeSink(args[0], fmt);
	      }
	    };
	  }
}
