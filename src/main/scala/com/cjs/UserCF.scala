package com.cjs

import breeze.numerics.{pow, sqrt}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UserCF {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

        val conf = new SparkConf()
            .set("spark.some.config.option","some-value")

        val ss = SparkSession
            .builder()
            .config(conf)
            .enableHiveSupport()
            .appName("test_USER_CF")
            .master("local[2]") //单机版配置，集群情况下，可以去掉
            .getOrCreate()

        val userDataDF = ss.sql("select user_id, item_id, rating from bigdatas.udata")

//        1、计算相似用户，使用cosine = a*b/(|a|*|b|)
//        1.1、计算分母
        import ss.implicits._
        val userScoreSum = userDataDF.rdd.map(x=>(x(0).toString,x(2).toString))
            .groupByKey()
            .mapValues(x=>sqrt(x.toArray.map(rating=>pow(rating.toDouble,2)).sum))
            .toDF("user_id","rating_sqrt_sum")
//        1.2、倒排表(基于item的笛卡儿积)
        val vDataDF = userDataDF.selectExpr("user_id as user_v", "item_id", "rating as rating_v")
        val u_v_decare = userDataDF.join(vDataDF,"item_id")
            .filter("case(rating as long)<>case(rating_v as long)")
//        1.3、计算分子,维度值（rating）点乘,累加求和
        val df_product = u_v_decare.selectExpr("item_id","user_id","user_v","case(rating as double)*case(rating_v as double) as prod")
        val df_sim_group = df_product.groupBy("user_id","user_v")
            .agg("prod"->"sum")
            .withColumnRenamed("sum(prod)","rating_dot")

//        1.4、计算整个cosine
        val vScoreSum = userScoreSum.selectExpr("user_id as user_v","rating_sqrt_sum as rating_sqrt_sum_v")
        val df_sim_cosine = df_sim_group
            .join(userScoreSum,"user_id")
            .join(vScoreSum,"user_v")
            .selectExpr("user_id","user_v","rating_dot/(rating_sqrt_sum*rating_sqrt_sum_v) as cosine_sim")
//        2、获取相似用户的物品集合
//        2.1、取得每个用户的前n个相似用户
        val sim_user_topN = df_sim_cosine.rdd.map(row=>(row(0).toString,(row(1).toString,row(2).toString)))
            .groupByKey()
            .mapValues(_.toArray.sortWith((x,y)=>x._2>y._2).slice(0,10))    //列转行， RDD[(String, Array[(String, String)])]
            .flatMapValues(x=>x)    //行转列， RDD[(String, (String, String))]
            .toDF("user_id","user_v_sim")
            .selectExpr("user_id","user_v_sim._1 as user_v","user_v_sim._2 as cosine_sim")//将一个tuple的字段拆分成两个字段
/**
        df_sim_cosine.createOrReplaceTempView("df_sim_cosine")
  //不可以这样做，因为df_sim_cosine数据里面包含了所有用户
        val sim_user_topN_tmp = ss.sql("select user_id, user_v, cosine_sim from df_sim_cosine order by cosine_sim desc limit 10")
  **/
//        2.2、获取相似用户的物品集合，并进行过滤
//        获取user_id物品集合（同时也可以获取user_v的物品集合）
        val df_user_items = userDataDF.rdd.map(row=>(row(0).toString,row(1).toString+"_"+row(2).toString))
            .groupByKey()
            .mapValues(_.toArray)
            .toDF("user_id","item_rating_arr")

        val df_user_items_v = df_user_items.selectExpr("user_id as user_id_v", "item_rating_arr as item_rating_arr_v")
        //依次基于user_id、user_v聚合
        val df_gen_item = sim_user_topN
            .join(df_user_items,"user_id")
            .join(df_user_items_v,"user_v")

//        2.3、用一个udf从user_v的商品集合中，将与user_id具有相同的商品过滤掉,得到候选集
        import org.apache.spark.sql.functions._
        val filter_udf = udf{(items:Seq[String],items_v:Seq[String])=>
            val fMap = items.map{x=>
                val l = x.split("_")
                (l(0),l(1))
            }.toMap
            //过滤商品
            items_v.filter{x=>
                val l = x.split("_")
                fMap.getOrElse(l(0),-1) == -1
            }
        }
        //过滤掉user_id商品的DF数据（user_id, consine_sim, item_rating）
        val df_filter_item = df_gen_item.withColumn("filtered_item", filter_udf(df_gen_item("item_rating_arr"),df_gen_item("item_rating_arr_v")))
            .select("user_id","cosine_sim", "filtered_item")

//        2.4、公式计算：①用户相似度*②rating
        val simRatingUDF = udf{(sim:Double,items:Seq[String])=>
            items.map{item_rating=>
                val l = item_rating.split("_")
                l(0)+"_"+l(1).toDouble*sim
            }
        }
        //DF:(user_id,item_prod)
        val itemSimRating = df_filter_item.withColumn("item_prod",simRatingUDF(df_filter_item("cosine_sim"),df_filter_item("filtered_item")))
            .select("user_id","item_prod")
        //行转列Array[item_prod],并分割item_pro。
        // 注意：得出的数据结果，会出现多个相同的user_id->item,因为同一个user_id的不同相似用户，可能会有同一样商品，分割后，就出现这情况
        val userItemScore = itemSimRating.select(itemSimRating("user_id"),explode(itemSimRating("item_prod")))
            .selectExpr("user_id","split('item_prod','_')[0] as item_id","case(split('item_prod','_')[1] as double) as score")//将一个字符串的字段拆分成两个字段

        //基于user_id和item_id做聚合
        val userItemScoreSum = userItemScore.groupBy("user_id","item_id")
            .agg("score"->"sum")
            .withColumnRenamed("sum(score)","last_score")
        //排序取topN商品
        val df_rec = userItemScoreSum.rdd.map(row=>(row(0),(row(1).toString,row(2).toString)))
            .groupByKey()
            .mapValues(_.toArray.sortWith((x,y)=>x._2>y._2).slice(0,10))
            .flatMapValues(x=>x)
            .toDF("user_id","item_sim")
            .selectExpr("user_id","item_sim._1 as item_id", "item_sim._2 as score")
    }

}
