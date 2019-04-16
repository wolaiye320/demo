package com.sougen;

import com.dangdang.ddframe.job.config.JobCoreConfiguration;
import com.dangdang.ddframe.job.config.JobRootConfiguration;
import com.dangdang.ddframe.job.config.simple.SimpleJobConfiguration;
import com.dangdang.ddframe.job.lite.api.JobScheduler;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperConfiguration;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

/**
 * @Auther: sam
 * @Date: 2019/3/22
 * @Description:
 * @return
 */
public class SimpleJobDemoTest {
    public static void main(String[] args) {
        new JobScheduler(createRegistryCenter(), createJobConfiguration("A")).init();
    }

    private static CoordinatorRegistryCenter createRegistryCenter() {
        //192.168.136.128:2181 这个为zk的地址
        //demo-job 这个为1个zk环境的下的1个namespace 可以有多个 1个namespace下有多个job
        CoordinatorRegistryCenter regCenter = new ZookeeperRegistryCenter(
                new ZookeeperConfiguration("192.168.136.128:2181", "demo-job"));
        regCenter.init();
        return regCenter;
    }
    private static LiteJobConfiguration createJobConfiguration(String jobParameter) {
        // mySimpleTest 为jobname 0/10 * * * * ?为cron表达式  2 分片数量  0=北京,1=上海 分片对应内容  jobParameter 自定义参数
        JobCoreConfiguration simpleCoreConfig = JobCoreConfiguration.newBuilder("simpleJobDemo", "0/10 * * * * ?", 2).shardingItemParameters("0=北京,1=上海").jobParameter(jobParameter).build();
        SimpleJobConfiguration simpleJobConfig = new SimpleJobConfiguration(simpleCoreConfig, SimpleJobDemo.class.getCanonicalName());
        LiteJobConfiguration simpleJobRootConfig = LiteJobConfiguration.newBuilder(simpleJobConfig).build();

        return  simpleJobRootConfig;
    }

}
