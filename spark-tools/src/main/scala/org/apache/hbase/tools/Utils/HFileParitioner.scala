package org.apache.hbase.tools.Utils

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.KeyValue.KVComparator
import org.apache.hadoop.hbase.client.HRegionLocator
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.WritableComparator
import org.apache.spark.Partitioner

class HFileParitioner(startKeys :Array[Array[Byte]]) extends Partitioner{
  require(startKeys!=null && (startKeys.size > 0))
  private val regionAndIndex = startKeys.sorted(new Ordering[Array[Byte]]{
    override def compare(x: Array[Byte], y: Array[Byte]): Int = WritableComparator.compareBytes(x,0,x.length,y,0,y.length)
  })

  override def numPartitions: Int = startKeys.size

  override def getPartition(key: Any): Int = {
    // binary search sorted  regionAndIndex  to find the final position
    require(key.isInstanceOf[ImmutableBytesWritable])
    binarySearch(key.asInstanceOf[ImmutableBytesWritable])
  }
  private def binarySearch(key:ImmutableBytesWritable): Int ={
    var start = 0
    var end = numPartitions - 1
    var mid = start + end / 2
    while(start < end){

      if(compare(regionAndIndex(start),key) > 0 ){
        start = -1
        throw new Exception("key value less than the start region startkey")
      } else if(compare(regionAndIndex(start),key) == 0 ){
          return start
      }else{
        // key > start
          if(start == mid ){
            return start
          }
          if(compare(regionAndIndex(end),key) <= 0 ){
            // key >= end
              start = end
              return start
          }else{
            // start < key < end

            if(compare(regionAndIndex(mid),key) > 0 ){
              end = mid
            }else if(compare(regionAndIndex(mid),key) == 0){
              start = mid
              return start
            }else{
              start = mid
            }
          }
        mid = ( start + end ) / 2
      }
    }
    return start
  }

  private def compare(x: Array[Byte], y: ImmutableBytesWritable): Int = WritableComparator.compareBytes(x,0,x.length,y.get(),y.getOffset,y.getLength)
}
