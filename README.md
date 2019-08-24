# UserCF_Spark
<p>基于Spark实现User的协同过滤CF</p>
<p>    原始数据u.data格式为（user_id,item_id,rating,timestamp）,这所有的数据来源是通过943个用户对1682个items进行打分。</p>
<p>    计算用户相似度时，使用了向量的余弦定理cosine。</p>
