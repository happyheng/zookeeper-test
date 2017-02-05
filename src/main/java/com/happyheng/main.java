package com.happyheng;

import com.happyheng.zookeeper.CreateGroup;
import com.happyheng.zookeeper.JoinGroup;
import com.happyheng.zookeeper.ZooKeeperHelper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 *
 * Created by happyheng on 17/1/19.
 */
public class main {

    public static void main(String[] args) throws Exception{

        //createNode();

        //createData();

        judge();

        /**
         * 具体可以看ZooKeeper这个类的方法,已经将很多操作都封装好了
         */
    }


    /**
     * 创建node节点
     */
    public static void createNode() throws Exception{

        CreateGroup createGroup = new CreateGroup();

        String groupName = "group1";
        createGroup.connect();
        createGroup.create(groupName);
        createGroup.close();

        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect();
        joinGroup.join(groupName + "/192.168.1.1");
        Thread.sleep(Long.MAX_VALUE);
    }


    /**
     * 为一个节点写入数据
     */
    public static void createData() throws Exception{

        // 1.建立连接
        ZooKeeperHelper helper = new ZooKeeperHelper();
        helper.connect();

        // 2.写入数据
        ZooKeeper zooKeeper = helper.getZooKeeper();
        byte[] nodeData = "hello zookeeper".getBytes("utf-8");
        String path = zooKeeper.create("/data", nodeData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建节点的路径为" + path);

        // 3.关闭连接
        helper.close();
    }


    public static void judge() throws Exception{

        // 1.建立连接
        ZooKeeperHelper helper = new ZooKeeperHelper();
        helper.connect();

        // 2.判断是否有/data节点
        ZooKeeper zooKeeper = helper.getZooKeeper();
        Stat stat = zooKeeper.exists("/data1", false);

        if (stat != null) {
            System.out.println("判断结果为" + stat);
        } else {
            System.out.println("无此节点");
        }


        // 3.得到一个节点下面的所有节点
        List<String> nodeList = zooKeeper.getChildren("/", false);
        System.out.println("节点列表为" + nodeList);


        // 4.得到一个节点中的数据
        Stat dataStat = new Stat();
        byte[] nodeData = zooKeeper.getData("/data", false, dataStat);
        String nodeString = new String(nodeData, "utf-8");
        System.out.println("节点中的数据为" + nodeString);
        //   4.1 Stat即是记录节点信息的实体类,包括创建时间信息,数据长度等,传入一个新的Stat对象,zookeeper得到相应节点数据后,
        // 会将数据保存至传入的Stat对象中
        //   4.2 注意,当path不正确的时候,会报出NoNodeException

        // 关闭连接
        helper.close();

    }


}
