package com.happyheng.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *
 * Created by happyheng on 17/1/19.
 */
public class CreateGroup implements Watcher{

    private static final String ZOOKEEPER_HOST = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);


    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
            connectedSignal.countDown();
        }
    }


    public void create(String groupName) throws KeeperException, InterruptedException {
        String path = "/" + groupName;
        String createPath = zooKeeper.create(path,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建节点的路径为" + createPath);
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public static void main(String[] args) throws Exception{
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect();
        createGroup.create("test");
        createGroup.close();
    }
}
