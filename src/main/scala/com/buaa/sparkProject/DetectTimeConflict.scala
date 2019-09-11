package com.buaa.sparkProject
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD;

object DetectTimeConflict {

  //判断时间是否在2018年6-8月份中
  def isInPeriod(date: String): Boolean = {
    var flag:Boolean = false;

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    val initDay: String = "2018-06-01";

    val curDate: Date = sdf.parse(date);
    val initDate: Date = sdf.parse(initDay);

    //一周为604800秒,6-8月份共7948800秒
    val timeInterval: Long = (curDate.getTime() - initDate.getTime())/1000;

    if(timeInterval >= 0 && timeInterval < 7948800) {
      flag = true;
    }

    return flag;

  }

  def isInTimeInterval (inStation: String, outStation: String, curTime: String): Boolean = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    var flag: Boolean = false;

    val inDate :Date = sdf.parse(inStation);
    val outDate :Date = sdf.parse(outStation);
    val curDate :Date = sdf.parse(curTime);

    //计算出的时间差额单位为秒
    val ab_cur_in = curDate.getTime() - inDate.getTime();
    val ab_out_cur = outDate.getTime() - curDate.getTime();

    if(ab_cur_in >= 0 && ab_out_cur >= 0) {
      flag = true;
    }

    return flag;
  }

  def formateDate (s: String): String = {
    var res: String = "";
    if(s.indexOf("T") != -1) {
      //说明是入站时间 2018-06-10T19:16:05
      res = s.replace('T', ' ');
    }else {
      //说明是出站时间 20180610200538
      val year: String = s.substring(0,4);
      val mon: String = s.substring(4,6);
      val day: String = s.substring(6,8);
      val hour: String = s.substring(8,10);
      val min: String = s.substring(10,12);
      val sec: String = s.substring(12,14);
      res = year + "-" + mon + "-" + day + " " + hour + ":" + min + ":" + sec;
    }
    return res;
  }

  /**
    * 根据上一步筛选出的日消费总额超过3万的所有车辆出行记录，检测同一车牌出行记录是否存在出行时间冲突的数据，若冲突数据超过三条，被判断为异常数据提取出来。
    * @param inPath 日收入超3万的所有车牌的出行记录
    * @param outPath	输出文件内容：输入的出行记录存在时间冲突的所有车牌，每条车牌后跟了三条存在冲突的出入站时间数据
    */
  def detectTimeConflict (abline: RDD[String]): RDD[String] = {
    var timeMap = Map[String, List[String]]();//以车牌为ID，以进入站时间作为value

    //筛选不完整记录
    val filte = abline.filter{x=>
      val items = x.split(",")
      items.length == 6
    }
    println("记录总数" + filte.count())

    //将每条记录的出口收费车牌、出入站时间保留到RDD中  映射键值对（key:车牌,value:(入站时间，出站时间)）
      val mapped = filte.map{ x=>
        val items = x.split(",")
        val plate = items(5)//车牌号
        //println(plate)
        val outDate = formateDate(items(0).substring(21,35))
        val inDate = formateDate(items(3))
        (plate,(inDate,outDate))
      }

    //mapped1结构：(plate,((inDate,outDate),(inDate,outDate),...))
    val mapped1 = mapped.groupByKey();
    println("车牌总数" + mapped1.count())

    //进行时间冲突检测
    val mapped2 = mapped1.filter{x=>
      val len = x._2.size;
      var cir1: Boolean = true;
      var cir2: Boolean = true;

      var flag:Boolean = false;
      var num:Int = 0;
      val v = x._2.toArray;

      for(i <- 0 to (len-2) if cir1) {
        val inStation = v(i)._1;
        val outStation = v(i)._2;

        val dt = inStation.substring(0,10)
        if(isInPeriod(dt)){
          for(j <- (i+1) to (len-1) if cir2){
            val curT_1 = v(j)._1;
            val curT_2 = v(j)._2;
            if(isInTimeInterval(inStation, outStation, curT_1) || isInTimeInterval(inStation, outStation, curT_2)){
              //说明存在冲突
              num += 1;
              if(num == 3) {
                flag = true;
                num = 0;
                cir2 = false;
              }
            }
          }
          if(flag){
            cir1 = false;
          }
        }
      }
      flag;
    }

  //返回仅包含键的RDD
    return mapped2.keys;

  }

  def main(args: Array[String]) {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " 程序开始执行")

    //Spark 数据分析的程序入口SparkContext,用于读取数据
    //读取Spark Application 的配置信息
    val sparkConf = new SparkConf()
      //设置SparkApplication名称
      .setAppName("sparkProject")
      //设置程序运行的环境，通常情况下，在IDE中开发的时候，设置为local mode，至少是两个Thread
      //在实际部署的时候通过提交应用的命令去进行设置
      //setMaster("local[1]")
    val sc = SparkContext.getOrCreate(sparkConf);

    //读取车辆的异常记录
    var abline = sc.textFile("hdfs://bigdata01:9000/home/pq/scala/abnormalPlateRec.csv");
    //var abline = sc.textFile("I:\\programData\\scala\\abnormalPlateRec(3万).csv");
    val header = abline.first()
    abline = abline.filter(row => row != header)
    //spark.read.format("csv").option("header","true").csv(path-to-file)

    val resRDD = detectTimeConflict(abline);

    //保存文件
    resRDD.repartition(1).saveAsTextFile("hdfs://bigdata01:9000/home/pq/scala/resAll");
    //resRDD.saveAsTextFile("I:\\programData\\scala\\res");

    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " 数据处理结束")
  }
}
