package com.atguigu.Utils;

import org.eclipse.jetty.util.BlockingArrayQueue;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @PackageName:com.atguigu.Utils
 * @ClassNmae:ThreadPoolUtil
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/31 18:51
 */


public class ThreadPoolUtil {
    private  static ThreadPoolExecutor threadPoolExecutor=null;

    private ThreadPoolUtil() {
    }

    public  static ThreadPoolExecutor getThreadPoolExcutor(){
        if (threadPoolExecutor==null){
            synchronized (ThreadPoolUtil.class){
                if (threadPoolExecutor==null){
                    threadPoolExecutor = new ThreadPoolExecutor(4,20,5, TimeUnit.MINUTES,new LinkedBlockingQueue<>());

                }

            }

        }
        return threadPoolExecutor;

    }
}
