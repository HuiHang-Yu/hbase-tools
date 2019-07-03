package org.apache.hbase.tools

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client._
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.KeyValue.KVComparator
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.TableName
import org.apache.hbase.tools.Utils.HFileParitioner
import org.apache.spark.sql.SparkSession

object SparkBulkloadHBaseDemo {

  def main(args: Array[String]): Unit = {
    //decide to extract the config infomation to spark config  --conf
    System.setProperty("user.name","hbase")
    System.setProperty("HADOOP_USER_NAME","hbase")
    val conf = new SparkConf().setAppName("spark2HFile").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").registerKryoClasses(Array[Class[_]](classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[Put], classOf[ImmutableBytesWritable.Comparator],classOf[KVComparator]))
    val tmpOutPut = conf.get("spark.app.conf.output",null)
    require(tmpOutPut != null)
    val zk = conf.get("spark.app.conf.zk",null)
    require(zk != null)
    val zkPort = conf.get("spark.app.conf.zk.port","2181")
    val zkZnode = conf.get("spark.app.conf.zk.znode",null)
    require(zkZnode != null)
    val table = conf.get("spark.app.conf.hbase.table",null)
    require(table != null)

    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val hconf = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum",zk) // zookeeper
    hconf.set("hbase.zookeeper.property.clientPort",zkPort) // port
    hconf.set("zookeeper.znode.parent",zkZnode) // znode
    hconf.set(TableOutputFormat.OUTPUT_TABLE, table)
    val job = Job.getInstance()
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    val conn = ConnectionFactory.createConnection(hconf)
    val hbTableName = TableName.valueOf(table.getBytes)
    val regionLocator = new HRegionLocator(hbTableName, conn.asInstanceOf[ClusterConnection])
    val realTable = conn.getTable(hbTableName)
    HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator)  //decide reducer class and not decide the partition number so we need to repartition by regionlocator
    val startKeys = regionLocator.getStartKeys
    val num = sc.parallelize(1 to 10).repartition(1).map(x => String.valueOf(x))
    val comparator = new KVComparator()
    val kvComparator = sc.broadcast(comparator)
    implicit val compare = new Ordering[KeyValue]{
      override def compare(x:  KeyValue, y:  KeyValue): Int = {
        kvComparator.value.compare(x.getKey(),0,x.getKeyLength(),y.getKey(),0,y.getKeyLength())
      }
    }
    val rdd = num.map(x=>{
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "f1".getBytes(), "test".getBytes(), "value_xxx".getBytes() )
      (new ImmutableBytesWritable(Bytes.toBytes(x)), kv)
    }).partitionBy(new HFileParitioner(startKeys)).sortBy(x=>x._2)   //need  to repartition

      rdd.saveAsNewAPIHadoopFile(tmpOutPut,classOf[ImmutableBytesWritable], classOf[KeyValue],classOf[HFileOutputFormat2], job.getConfiguration())
  // bulk load start// bulk load start
    val loader =  new LoadIncrementalHFiles(hconf)
    val admin: Admin = conn.getAdmin
    loader.doBulkLoad(new Path(tmpOutPut), admin, realTable, regionLocator)
  }
}
