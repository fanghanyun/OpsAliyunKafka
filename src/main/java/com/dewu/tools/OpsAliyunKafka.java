package com.dewu.tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.alikafka.model.v20190916.*;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.google.gson.Gson;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class OpsAliyunKafka {
    /**
     * 阿里云接口鉴权
     *
     * @return
     */
    public static IAcsClient getProfile(String accessKeyId,String secret) {
        DefaultProfile profile = DefaultProfile.getProfile("cn-hangzhou", accessKeyId, secret);
        IAcsClient client = new DefaultAcsClient(profile);
        return client;
    }

    /**
     * 遍历杭州region，返回所有kafka实例ID
     *
     * @return
     */
    public static List<String> getInstanceID(String accessKeyId,String secret) {
        List<String> instances = new ArrayList<String>();
        IAcsClient client = getProfile(accessKeyId,secret);
        CommonRequest request = new CommonRequest();
        request.setSysMethod(MethodType.POST);
        request.setSysDomain("alikafka.cn-hangzhou.aliyuncs.com");
        request.setSysVersion("2019-09-16");
        request.setSysAction("GetInstanceList");
        request.putQueryParameter("RegionId", "cn-hangzhou");
        try {
            CommonResponse response = client.getCommonResponse(request);
            String content = response.getData();
            JSONObject jsonObject = JSON.parseObject(content);
            JSONObject InstanceList = jsonObject.getJSONObject("InstanceList");
            JSONArray jsonArray = InstanceList.getJSONArray("InstanceVO");
            for (int i = 0; i < jsonArray.size(); i++) {
                String InstanceId = jsonArray.getJSONObject(i).getString("InstanceId");
                instances.add(InstanceId);
            }
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return instances;
    }

    /**
     * 根据total总数，计算分页
     *
     * @param instances
     * @return
     */
    public static HashMap<String, Integer> getTopicNum(String accessKeyId,String secret,List<String> instances) {
        int page_num = 0;
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        for (String instance : instances) {
            IAcsClient client = getProfile(accessKeyId,secret);
            CommonRequest request = new CommonRequest();
            request.setSysMethod(MethodType.POST);
            request.setSysDomain("alikafka.cn-hangzhou.aliyuncs.com");
            request.setSysVersion("2019-09-16");
            request.setSysAction("GetTopicList");
            request.putQueryParameter("RegionId", "cn-hangzhou");
            request.putQueryParameter("CurrentPage", "1");
            request.putQueryParameter("PageSize", "10");
            //request.putQueryParameter("InstanceId", instance);
            request.putQueryParameter("InstanceId", "alikafka_post-cn-45912akmr001");
            try {
                CommonResponse response = client.getCommonResponse(request);
                JSONObject responseObject = JSON.parseObject(response.getData());
                String total = responseObject.getString("Total");
                int total_int = Integer.parseInt(total);
                page_num = total_int / 10 + 1;
                //System.out.println(instance+"..."+total+"..."+page_num);
                hashMap.put(instance, page_num);
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                e.printStackTrace();
            }
        }
        return hashMap;
    }

    /**
     * 根据total总数，计算分页
     *
     * @param instancesID
     * @return
     */
    public static HashMap<String, Integer> getTopicNumByID(String accessKeyId,String secret,String instancesID) {
        int page_num = 0;
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        IAcsClient client = getProfile(accessKeyId,secret);
        CommonRequest request = new CommonRequest();
        request.setSysMethod(MethodType.POST);
        request.setSysDomain("alikafka.cn-hangzhou.aliyuncs.com");
        request.setSysVersion("2019-09-16");
        request.setSysAction("GetTopicList");
        request.putQueryParameter("RegionId", "cn-hangzhou");
        request.putQueryParameter("CurrentPage", "1");
        request.putQueryParameter("PageSize", "10");
        //request.putQueryParameter("InstanceId", instance);
        request.putQueryParameter("InstanceId",instancesID );
        try {
            CommonResponse response = client.getCommonResponse(request);
            JSONObject responseObject = JSON.parseObject(response.getData());
            String total = responseObject.getString("Total");
            int total_int = Integer.parseInt(total);
            page_num = total_int / 10 + 1;
            //System.out.println(instance+"..."+total+"..."+page_num);
            hashMap.put(instancesID, page_num);
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return hashMap;
    }

    /**
     * 根据实例ID，分页数，遍历输出所有topic
     *
     * @param hashMap
     */
    public static void getAllTopic(String accessKeyId,String secret,HashMap<String, Integer> hashMap) {
        for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
            String instance = entry.getKey();
            int page = entry.getValue();
            for (int n = 1; n <= page; n++) {
                String current_page = String.valueOf(n);
                IAcsClient client = getProfile(accessKeyId,secret);
                CommonRequest request = new CommonRequest();
                request.setSysMethod(MethodType.POST);
                request.setSysDomain("alikafka.cn-hangzhou.aliyuncs.com");
                request.setSysVersion("2019-09-16");
                request.setSysAction("GetTopicList");
                request.putQueryParameter("RegionId", "cn-hangzhou");
                request.putQueryParameter("CurrentPage", current_page);
                request.putQueryParameter("PageSize", "10");
                request.putQueryParameter("InstanceId", instance);
                //request.putQueryParameter("InstanceId", "alikafka_post-cn-45912akmr001");
                try {
                    CommonResponse response = client.getCommonResponse(request);
                    JSONObject responseObject = JSON.parseObject(response.getData());
                    String total = responseObject.getString("Total");
                    JSONObject topicList = responseObject.getJSONObject("TopicList");
                    JSONArray topicArray = topicList.getJSONArray("TopicVO");
                    for (int i = 0; i < topicArray.size(); i++) {
                        String instanceID = topicArray.getJSONObject(i).getString("InstanceId");
                        String topic = topicArray.getJSONObject(i).getString("Topic");
                        String PartitionNum = topicArray.getJSONObject(i).getString("PartitionNum");
                        Date d = new Date();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
                        String fileName = sdf.format(d);
                        File file = new File("/tmp/"+instance+"_topic-" + fileName + ".txt");
                        file.createNewFile();
                        FileWriter writer = new FileWriter(file, true);
                        writer.write(instanceID + " " + total + " " + topic + " " + PartitionNum + System.getProperty("line.separator"));
                        writer.flush();
                        writer.close();
                    }
                } catch (ServerException e) {
                    e.printStackTrace();
                } catch (ClientException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void GetConsumerList(String accessKeyId,String secret,String instancesID){
        IAcsClient client = getProfile(accessKeyId,secret);
        GetConsumerListRequest request = new GetConsumerListRequest();
        request.setInstanceId(instancesID);
        request.setRegionId("cn-hangzhou");
        try {
            GetConsumerListResponse response = client.getAcsResponse(request);
            //System.out.println(new Gson().toJson(response));
            JSONObject responseObject = JSON.parseObject(new Gson().toJson(response));
            JSONArray consumerListJSONArrayArray = responseObject.getJSONArray("consumerList");
            for(int i=0;i<consumerListJSONArrayArray.size();i++){
                String consumerId=consumerListJSONArrayArray.getJSONObject(i).getString("consumerId");
                String instanceId=consumerListJSONArrayArray.getJSONObject(i).getString("instanceId");
                //System.out.println(consumerId+" "+instanceId);
                Date d = new Date();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
                String fileName = sdf.format(d);
                File file = new File("/tmp/"+instancesID+"_group-" + fileName + ".txt");
                file.createNewFile();
                FileWriter writer = new FileWriter(file, true);
                writer.write(instanceId + " " + consumerId + System.getProperty("line.separator"));
                writer.flush();
                writer.close();
            }
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (ClientException e) {
            System.out.println("ErrCode:" + e.getErrCode());
            System.out.println("ErrMsg:" + e.getErrMsg());
            System.out.println("RequestId:" + e.getRequestId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 阿里云kafka批量创建topic
     * 读取文本格式
     * alikafka_pre-cn-45911y852001 2 activity_seckill_20190410_test 24
     * @param accessKeyId
     * @param secret
     * @param instancesID
     * @param filePath
     * @throws IOException
     */
    public static void createTopic(String accessKeyId,String secret,String instancesID,String filePath) throws IOException {
        ArrayList<String> topics=new ArrayList<String>();
        File f=new File(filePath);
        FileInputStream fip = new FileInputStream(f);
        InputStreamReader reader = new InputStreamReader(fip, "UTF-8");
        StringBuffer sb = new StringBuffer();
        while (reader.ready()) {
            sb.append((char) reader.read());
            // 转成char加到StringBuffer对象中
        }
        String str=sb.toString();
        String[] lines=str.split("\\r?\\n");
        for(String line:lines){
            //读取文本，以空格切分，取第三列
            topics.add(line.split(" ")[2]);
        }
        for(String tp:topics){
            IAcsClient client = getProfile(accessKeyId,secret);
            CreateTopicRequest request = new CreateTopicRequest();
            request.setInstanceId(instancesID);
            request.setTopic(tp);
            request.setRemark(tp);

            try {
                CreateTopicResponse response = client.getAcsResponse(request);
                System.out.println(new Gson().toJson(response));
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                System.out.println("ErrCode:" + e.getErrCode());
                System.out.println("ErrMsg:" + e.getErrMsg());
                System.out.println("RequestId:" + e.getRequestId());
            }
        }
        reader.close();

    }

    /**
     * 阿里云kafka批量删除topic
     * 读取文本格式
     * alikafka_pre-cn-45911y852001 2 activity_seckill_20190410_test 24
     * @param accessKeyId
     * @param secret
     * @param instancesID
     * @param filePath
     * @throws IOException
     */
    public static void deleteTopic(String accessKeyId,String secret,String instancesID,String filePath) throws IOException {
        ArrayList<String> topics=new ArrayList<String>();
        File f=new File(filePath);
        FileInputStream fip = new FileInputStream(f);
        InputStreamReader reader = new InputStreamReader(fip, "UTF-8");
        StringBuffer sb = new StringBuffer();
        while (reader.ready()) {
            sb.append((char) reader.read());
            // 转成char加到StringBuffer对象中
        }
        String str=sb.toString();
        String[] lines=str.split("\\r?\\n");
        for(String line:lines){
            //读取文本，以空格切分，取第三列
            topics.add(line.split(" ")[2]);
        }
        for(String tp:topics){
            IAcsClient client = getProfile(accessKeyId,secret);
            DeleteTopicRequest request = new DeleteTopicRequest();
            request.setInstanceId(instancesID);
            request.setTopic(tp);

            try {
                DeleteTopicResponse response = client.getAcsResponse(request);
                System.out.println(new Gson().toJson(response));
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                System.out.println("ErrCode:" + e.getErrCode());
                System.out.println("ErrMsg:" + e.getErrMsg());
                System.out.println("RequestId:" + e.getRequestId());
            }
        }
        reader.close();

    }

    /**
     * 阿里云kafka批量创建consumer group
     * 读取文本格式
     * alikafka_pre-cn-45911y852001 php_api_activity_test
     * @param accessKeyId
     * @param secret
     * @param instancesID
     * @param filePath
     * @throws IOException
     */
    public static void createConsumerGroup(String accessKeyId,String secret,String instancesID,String filePath) throws IOException {
        ArrayList<String> groups=new ArrayList<String>();
        File f=new File(filePath);
        FileInputStream fip = new FileInputStream(f);
        InputStreamReader reader = new InputStreamReader(fip, "UTF-8");
        StringBuffer sb = new StringBuffer();
        while (reader.ready()) {
            sb.append((char) reader.read());
            // 转成char加到StringBuffer对象中
        }
        String str=sb.toString();
        String[] lines=str.split("\\r?\\n");
        for(String line:lines){
            //读取文本，以空格切分，取第二列
            groups.add(line.split(" ")[1]);
        }
        for(String group:groups){
            IAcsClient client = getProfile(accessKeyId,secret);
            CreateConsumerGroupRequest request = new CreateConsumerGroupRequest();
            request.setInstanceId(instancesID);
            request.setConsumerId(group);

            try {
                CreateConsumerGroupResponse response = client.getAcsResponse(request);
                System.out.println(new Gson().toJson(response));
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                System.out.println("ErrCode:" + e.getErrCode());
                System.out.println("ErrMsg:" + e.getErrMsg());
                System.out.println("RequestId:" + e.getRequestId());
            }
        }
        reader.close();

    }

    /**
     * 阿里云kafka批量删除consumer group
     * 读取文本格式
     * alikafka_pre-cn-45911y852001 php_api_activity_test
     * @param accessKeyId
     * @param secret
     * @param instancesID
     * @param filePath
     * @throws IOException
     */
    public static void deleteConsumerGroup(String accessKeyId,String secret,String instancesID,String filePath) throws IOException {
        ArrayList<String> groups=new ArrayList<String>();
        File f=new File(filePath);
        FileInputStream fip = new FileInputStream(f);
        InputStreamReader reader = new InputStreamReader(fip, "UTF-8");
        StringBuffer sb = new StringBuffer();
        while (reader.ready()) {
            sb.append((char) reader.read());
            // 转成char加到StringBuffer对象中
        }
        String str=sb.toString();
        String[] lines=str.split("\\r?\\n");
        for(String line:lines){
            //读取文本，以空格切分，取第二列
            groups.add(line.split(" ")[1]);
        }
        for(String group:groups){
            IAcsClient client = getProfile(accessKeyId,secret);
            DeleteConsumerGroupRequest request = new DeleteConsumerGroupRequest();
            request.setInstanceId(instancesID);
            request.setConsumerId(group);

            try {
                DeleteConsumerGroupResponse response = client.getAcsResponse(request);
                System.out.println(new Gson().toJson(response));
            } catch (ServerException e) {
                e.printStackTrace();
            } catch (ClientException e) {
                System.out.println("ErrCode:" + e.getErrCode());
                System.out.println("ErrMsg:" + e.getErrMsg());
                System.out.println("RequestId:" + e.getRequestId());
            }

        }
        reader.close();

    }

    public static void help(){
        System.out.println("******************************************");
        System.out.println("usage：java -cp OpsAliyunKafka-1.0-SNAPSHOT.jar com.dewu.tools.OpsAliyunKafka accessKeyId secret [COMMAND]");
        System.out.println("                                [GetByRegion]：save all hangzhou instances topic");
        System.out.println("                                [InstancesID GetByInstanceID]：save topic and consumer group by instancesID");
        System.out.println("                                [InstancesID CreateTopic filePath]：create topic from backup file by instancesID");
        System.out.println("                                [InstancesID CreateConsumerGroup filePath]：create group from backup file by instancesID");
        System.out.println("                                [InstancesID DeleteTopic filePath]：delete topic from backup file by instancesID");
        System.out.println("                                [InstancesID DeleteConsumerGroup filePath]：delete group from backup file by instancesID");
        System.out.println("******************************************");
    }

    public static void main(String[] args) throws IOException {
        if (args.length <2) {
            System.out.println("Please input parameters...");
            help();
            System.exit(-1);
        }
        String accessKeyId=args[0];
        String secret=args[1];
        if (args[2].equals("GetByRegion")){
            //获取阿里云杭州所有kafka实例topic和group
            getAllTopic(accessKeyId,secret,getTopicNum(accessKeyId,secret,getInstanceID(accessKeyId,secret)));
        }
        String instancesID = args[2];
        String action = args[3];
        if (action.equals("GetByInstanceID")) {
            System.out.println("alikafka topic备份保存在/tmp/"+instancesID+"_topic-日期.txt");
            getAllTopic(accessKeyId,secret,getTopicNumByID(accessKeyId,secret,instancesID));
            System.out.println("alikafka group备份保存在/tmp/"+instancesID+"_group-日期.txt");
            GetConsumerList(accessKeyId,secret,instancesID);
        } else if (args.length == 5 && action.equals("CreateTopic")){
            createTopic(accessKeyId,secret,instancesID,args[4]);
        } else if (args.length == 5 && action.equals("CreateConsumerGroup")) {
            createConsumerGroup(accessKeyId,secret,instancesID,args[4]);
        } else if (args.length == 5 && action.equals("DeleteConsumerGroup")){
            deleteConsumerGroup(accessKeyId,secret,instancesID,args[4]);
        } else if (args.length == 5 && action.equals("DeleteTopic")){
            deleteTopic(accessKeyId,secret,instancesID,args[4]);
        }
    }
}
