# UserCF_Spark
<p>基于Spark实现User的协同过滤CF</p>
<p>\t原始数据u.data格式为（user_id,item_id,rating,timestamp）,这所有的数据来源是通过943个用户对1682个items进行打分。</p>
<p>    u.data数据被加载到hive表里面，所以spark操作的数据源为hive</p>
<p>    计算用户相似度时，使用了向量的余弦定理cosine。</p>
<p>详细讲解：https://blog.csdn.net/weixin_39400271/article/details/100058486</p>
