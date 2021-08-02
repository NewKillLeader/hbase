package com.ls.sh.iot.bigdata.hbase

import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectArrayList, ObjectArrays}
import org.apache.hadoop.hbase.mapreduce.{RegionSizeCalculator, TableInputFormat, TableSplit}
import org.apache.hadoop.hbase.util.{Bytes, Strings}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext}
import org.apache.hadoop.net.DNS

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.net

class TableInputFormat10 extends TableInputFormat {


  //reverseDNS cache
  val reverseDNSCacheMap = new Object2ObjectOpenHashMap[InetAddress, String](20)

  //startKey集合
  val keys = new ObjectArrayList[String](10)


  {
    for (i <- 0 to 9) {
      keys.add(i.toString)
    }
  }

  @Override
  override def getSplits(context: JobContext): util.List[InputSplit]= {
    super
      .initialize(context)

    //TableName
    val tableName = super.getTable.getName

    //regionSizeCalculator 计算给定表的每个region 的大小，关于给定列族,以便于更好的调度task
    val regionsSizeCalculator = new RegionSizeCalculator(getRegionLocator,getAdmin)


    //预定义split ArrayList  填装TableSplit对象，主要属性为tableName，startRow，endRow，region信息
    val splits = new ObjectArrayList[InputSplit](10)

    //遍历startRow
    keys.forEach(x=>{
      //获取对应startRow的region信息  false为不进行重载，使用缓存信息
      val location = getRegionLocator().getRegionLocation(Bytes.toBytes(x),false)
      //获取InetAddress
      val inetSocketAddr = new InetSocketAddress(location.getHostname,location.getPort)
      //addr
      val addr = inetSocketAddr.getAddress
      val regionLocation = reverseDNS(addr)

      //regionName  TableSplit属性
      val regionName = location.getRegion.getRegionName
      //encodeRegionName  TableSplit属性
      val encodedRegionName = location.getRegion.getEncodedName
      //regionSize  计算region 对应列族大小
      val regionSize = regionsSizeCalculator.getRegionSize(regionName)

      //start end分别为加上 startRow endRow
      val startRow = Bytes.add(Bytes.toBytes(x+"_"),this.getScan.getStartRow)
      val endRow = Bytes.add(Bytes.toBytes(x+"_"),this.getScan.getStopRow)

      //TableSplit对象
      val tableSplit = new TableSplit(tableName,this.getScan,startRow,endRow,regionLocation,encodedRegionName,regionSize)
      splits.add(tableSplit)

    })

    splits
  }


  //自定义reverseDNS  使用dns缓存
  override def reverseDNS(inetAddress: InetAddress) = {
    var hostName = reverseDNSCacheMap.getOrDefault(inetAddress,null)
    if(hostName==null){
      //如果未缓存，则进行查找
      var ipAddressString = "null"
      try{
        ipAddressString = DNS.reverseDns(inetAddress,null)
      }catch{
        case ex:Exception=> {
          ex.printStackTrace()
          ipAddressString = InetAddress.getByName(inetAddress.getHostAddress).getHostName
        }
      }

      hostName = Strings.domainNamePointerToHostName(ipAddressString)
      this.reverseDNSCacheMap.put(inetAddress,hostName)
    }

    hostName
  }


}

