package com.lst.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HDFSCommon {

	/**
	 * 在hdfs更目录下面创建test1文件夹
	 * 
	 * @throws IOException
	 */
	@Test
	public void mkdir() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.10.96.210:9000");
		FileSystem fs = FileSystem.newInstance(conf);
		fs.mkdirs(new Path("/test1"));
	}

	/**
	 * 列出hdfs上所有的文件或文件夹
	 * 
	 * @throws IOException
	 */
	@Test
	public void listFiles() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.10.96.210:9000");
		FileSystem fs = FileSystem.newInstance(conf);
		// true 表示递归查找 false 不进行递归查找
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
		while (iterator.hasNext()) {
			LocatedFileStatus next = iterator.next();
			System.out.println(next.getPath());
		}
		System.out.println("----------------------------------------------------------");
		FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
		for (int i = 0; i < fileStatuses.length; i++) {
			FileStatus fileStatus = fileStatuses[i];
			System.out.println(fileStatus.getPath());
		}
	}


	/**
	 * 上传文件到hdfs上
	 */
	@Test
	public void upload() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.10.96.210:9000");
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path("/home/wlj/xzw/storm-core-1.2.2.jar"), new Path("/test1"));
	}

	/**
	 * 将hdfs上文件下载到本地
	 */
	@Test
	public void download() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.10.96.210:9000");
		FileSystem fs = FileSystem.newInstance(conf);
		fs.copyToLocalFile(new Path("/test1/storm-core-1.2.2.jar"), new Path("/home/wlj/xzw/download"));
	}

	/**
	 * 删除hdfs上的文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void removeFile() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://10.10.96.210:9000");
		FileSystem fs = FileSystem.newInstance(conf);
		fs.delete(new Path("/test1/storm-core-1.2.2.jar"), true);
	}
}
