package com.happyheng.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * zookeeper工具类
 * Created by happyheng on 17/1/21.
 */
public class ZooKeeperHelper implements Watcher{

    private static final String ZOOKEEPER_HOST = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 需先调用此方法连接zookeeper
     */
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_HOST, SESSION_TIMEOUT, this);
        connectedSignal.await(); // 阻塞主线程,等待与zookeeper的连接
    }

    /**
     * 当与zookeeper连接到后会回调此方法,判断如果是连接成功,那么释放主线程阻塞
     */
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected){
            connectedSignal.countDown();
        }
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    /**
     * 操作完成之后,可以调用此方法关闭与zookeeper的连接
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }


}
