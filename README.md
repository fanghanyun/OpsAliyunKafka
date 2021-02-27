#功能
封装阿里云kafka sdk操作topic&consumer group，实现的功能：备份，删除，新建。打成jar包运行时，只需传入ak及实例ID即可。

#运行效果，请按提示传入参数
java -cp OpsAliyunKafka-1.0-SNAPSHOT.jar com.dewu.tools.OpsAliyunKafka

Please input parameters...
******************************************
usage：java -cp OpsAliyunKafka-1.0-SNAPSHOT.jar com.dewu.tools.OpsAliyunKafka accessKeyId secret [COMMAND]
                                [GetByRegion]：save all hangzhou instances topic
                                [InstancesID GetByInstanceID]：save topic and consumer group by instancesID
                                [InstancesID CreateTopic filePath]：create topic from backup file by instancesID
                                [InstancesID CreateConsumerGroup filePath]：create group from backup file by instancesID
                                [InstancesID DeleteTopic filePath]：delete topic from backup file by instancesID
                                [InstancesID DeleteConsumerGroup filePath]：delete group from backup file by instancesID
******************************************
