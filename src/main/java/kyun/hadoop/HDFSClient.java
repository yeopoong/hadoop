package kyun.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSClient {
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://server:9000");

		FileSystem dfs = FileSystem.get(conf);

		System.out.println("Home Path : " + dfs.getHomeDirectory());
		System.out.println("Work Path : " + dfs.getWorkingDirectory());

		Path filenamePath = new Path("/tmp/hello.txt");
		System.out.println("File Exists : " + dfs.exists(filenamePath));

		if (dfs.exists(filenamePath)) {
			dfs.delete(filenamePath, true);
		}

		FSDataOutputStream out = dfs.create(filenamePath);
		out.writeUTF("Hello, world!\n");
		out.close();

		FSDataInputStream in = dfs.open(filenamePath);
		String messageIn = in.readUTF();
		System.out.print(messageIn);

		in.close();
		dfs.close();
	}
}
