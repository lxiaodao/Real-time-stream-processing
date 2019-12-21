/**
 * 
 */
package com.saas.bigdata.hbase;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import com.google.common.primitives.Bytes;

/**
 * @author yangliu
 *
 */
public class HbaseClient {

	private static final String TABLE_NAME = "item";
	private static final String CF_DEFAULT = "shortName";

	public static void scan_action(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		// Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(HbaseClient.TABLE_NAME);
		Table table = connection.getTable(tableName);
		//
		String family1 = "shortName";
		Scan scan = new Scan();
		scan.addFamily(family1.getBytes());

		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("Found row: " + result);

			// byte[] rowkey=result.getRow();
			// If qualifier is null,is ""
			System.out.println("---value---: " + new String(result.getValue(family1.getBytes(), "".getBytes())));
		}
	}

	public static void query_scan(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		// Admin admin = connection.getAdmin();

		TableName tableName = TableName.valueOf(HbaseClient.TABLE_NAME);
		Table table = connection.getTable(tableName);
		Scan scan = new Scan();
		String value = "sumsung Glaxy10";
		String family = "shortName";
		Filter filter = new SingleColumnValueFilter(family.getBytes(), "".getBytes(), CompareOperator.EQUAL,
				value.getBytes());
		scan.setFilter(filter);

		ResultScanner rs = table.getScanner(scan);
		
		for (Result result : rs) {
			
			System.out.println("---value---: " + result);
		}

	}

	public static void main(String... args) throws IOException {
		Configuration config = HBaseConfiguration.create();

		// Add any necessary configuration files (hbase-site.xml, core-site.xml)

		URL url = HbaseClient.class.getClassLoader().getResource("hbase-site.xml");

		String path = url.getPath();
		config.addResource(new Path(path));
		HBaseAdmin.available(config);

		//HbaseClient.scan_action(config);
		HbaseClient.query_scan(config);

	}

}
