package com.happyheng.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *
 * Created by happyheng on 17/1/19.
 */
public class JoinGroup implements Watcher {


    private static final String ZOOKEEPER_HOST = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);


    /**
     * -------------
     * zookeeper创建node的第二个参数即为data,即为此节点要写入的数据
     * -------------
     */

    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
            connectedSignal.countDown();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }


    public void join(String name) throws KeeperException, InterruptedException {

        String path = "/" + name;
        String createdPath = zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("创建的路径为" + createdPath);

    }

    public static void main(String[] args) throws Exception{
        JoinGroup joinGroup = new JoinGroup();
        joinGroup.connect();
        joinGroup.join("memberNode");

        Thread.sleep(Long.MAX_VALUE);
    }

}
