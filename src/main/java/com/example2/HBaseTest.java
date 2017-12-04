package com.example2;

/**
 * Created by jieping_yang on 2017/6/22.
 */


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;


public class HBaseTest {
    private static final String TABLE_NAME = "scores";

    public static Configuration conf = null;
    public HTable table = null;
    public HBaseAdmin admin = null;
    public static Connection connection;


    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", "master,worker");
            conf.set("hbase.master", "10.64.66.215:60000");
            connection = ConnectionFactory.createConnection(conf);
            System.out.println(conf.get("hbase.zookeeper.quorum"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * create table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        Admin admin = connection.getAdmin();
        List<HRegionInfo> list = admin.getTableRegions(TableName.valueOf(tableName));
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * delete table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            Admin admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    /**
     * insert data
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            // HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * delete record
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List list = new ArrayList();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * query record
     */
    public static void getOneRecord(String tableName, String rowKey)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for (KeyValue kv : rs.raw()) {
            System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            System.out.print(kv.getTimestamp() + " ");
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * show data
     */
    public static void getAllRecord(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan s = new Scan();

            ResultScanner ss = table.getScanner(s);
            for (Result r : ss) {
                for (KeyValue kv : r.raw()) {
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public static void wordCount(JavaSparkContext sc){
        JavaRDD<String> lines = sc.textFile("hdfs://master:9000/Hadoop/Input/test.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                String[] words=s.split(" ");
                return Arrays.asList(words).iterator();
            }
        });
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub

        SparkConf conf1 = new SparkConf().setAppName(
                "DrCleaner_Retention_Rate_Geo_2_to_14").set("spark.executor.memory", "2000m").setMaster(
                "spark://10.64.66.215:7077").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setJars(new String[]{"/D:/Perforce2/Core/TMMSMDM/Dev/TMMSMDM_Android-9.8/Tmms4Android/JniTest2/hbase/build/libs/hbase.jar"});
        conf1.set("spark.cores.max", "4");
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config(conf1)
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        wordCount(sc);

        try {

            String tablename = "scores";
            String[] familys = {"grade", "course"};

            HBaseTest.creatTable(tablename, familys);

            // add record zkb
            HBaseTest.addRecord(tablename, "zkb1", "grade", "junior", "95");
            HBaseTest.addRecord(tablename, "zkb1", "course", "math", "88");
            HBaseTest.addRecord(tablename, "zkb2", "grade", "junior", "101");
            HBaseTest.addRecord(tablename, "zkb2", "course", "math", "87");
            HBaseTest.addRecord(tablename, "zkb3", "grade", "junior", "110");
            HBaseTest.addRecord(tablename, "zkb3", "course", "math", "97");
            // add record baoniu
            HBaseTest.addRecord(tablename, "baoniu", "grade", "junior", "4");
            HBaseTest
                    .addRecord(tablename, "baoniu", "course", "math", "89");

            System.out.println("===========get one record========");
            HBaseTest.getOneRecord(tablename, "zkb");

            System.out.println("===========show all record========");
            HBaseTest.getAllRecord(tablename);

            System.out.println("===========del one record========");
            HBaseTest.delRecord(tablename, "baoniu");
            HBaseTest.getAllRecord(tablename);

            System.out.println("===========show all record========");
            HBaseTest.getAllRecord(tablename);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {

            Scan scan = new Scan();
            //  scan.setStartRow(Bytes.toBytes("195861-1035177490"));
            //  scan.setStopRow(Bytes.toBytes("195861-1072173147"));

            scan.addColumn(Bytes.toBytes("grade"), Bytes.toBytes("junior"));
            scan.addColumn(Bytes.toBytes("course"), Bytes.toBytes("math"));
            List<Filter> filters = new ArrayList<Filter>();
            // RegexStringComparator comp = new RegexStringComparator("87"); // 以 you 开头的字符串
            //  SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("grade"), Bytes.toBytes("junior"), CompareFilter.CompareOp.EQUAL, comp2);
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("course"),
                    Bytes.toBytes("math"),
                    CompareFilter.CompareOp.GREATER, Bytes.toBytes("88"));
            filter.setFilterIfMissing(true);//if set true will skip row which column doesn't exist
            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(Bytes.toBytes("grade"),
                    Bytes.toBytes("junior"),
                    CompareFilter.CompareOp.LESS, Bytes.toBytes("111"));
            filter2.setFilterIfMissing(true);

            PageFilter filter3 = new PageFilter(10);
            filters.add(filter);
            filters.add(filter2);
            filters.add(filter3);
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);

            scan.setFilter(filterList);
            conf.set(TableInputFormat.INPUT_TABLE, "scores");
            conf.set(TableInputFormat.SCAN, convertScanToString(scan));

            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf,
                    TableInputFormat.class, ImmutableBytesWritable.class,
                    Result.class);
            long count = hBaseRDD.count();
            System.out.println("count: " + count);

            JavaRDD<Person> datas2 = hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Person>() {
                @Override
                public Person call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    Result result = immutableBytesWritableResultTuple2._2();
                    byte[] o = result.getValue(Bytes.toBytes("course"), Bytes.toBytes("math"));
                    if (o != null) {
                        Person person = new Person();
                        person.setAge(Long.parseLong(Bytes.toString(o)));
                        person.setName(Bytes.toString(result.getRow()));

                        return person;
                    }
                    return null;
                }
            });



          /* List<Tuple2<ImmutableBytesWritable, Result>> tuples = hBaseRDD
                    .take(count.intValue());
            for (int i = 0, len = count.intValue(); i < len; i++) {
                Result result = tuples.get(i)._2();
                KeyValue[] kvs = result.raw();
                for (KeyValue kv : kvs) {
                    System.out.println("rowkey:" + new String(kv.getRow()) + " cf:"
                            + new String(kv.getFamily()) + " column:"
                            + new String(kv.getQualifier()) + " value:"
                            + new String(kv.getValue()));
                }
            }*/


            Dataset<Row> data = spark.createDataFrame(datas2, Person.class);

            data.show();

            Job newAPIJobConfiguration1 = Job.getInstance(conf);
            newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "scores");
            newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

            // create Key, Value pair to store in HBase
            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = data.javaRDD().mapToPair(
                    new PairFunction<Row, ImmutableBytesWritable, Put>() {
                        @Override
                        public Tuple2<ImmutableBytesWritable, Put> call(Row row) throws Exception {

                            Put put = new Put(Bytes.toBytes(row.<String>getAs("name")));//row key
                            put.add(Bytes.toBytes("course"), Bytes.toBytes("math"), Bytes.toBytes(String.valueOf(row.<Long>getAs("age"))));

                            return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
                        }
                    });

            // save to HBase- Spark built-in API method
            hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
            spark.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
