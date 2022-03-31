package com.ruoyi.ruoyidiffkit;

import com.ruoyi.ruoyidiffkit.service.Index;
import com.ruoyi.ruoyidiffkit.service.QueryService;
import com.ruoyi.ruoyidiffkit.service.ConnectionForCompare;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class })
public class RuoyiDiffkitApplication {

    @Autowired
    private QueryService queryService;

    public static void main(String[] args) {
        SpringApplication.run(RuoyiDiffkitApplication.class, args);
    }

    @PostConstruct
    public void init() {
        // 计算耗时
        long startTime = System.currentTimeMillis();
        // 构造数据源MetaConfig对象
        MetaConfig metaConfig = new MetaConfig("cluster.lauvinson.com", 30306, "root", "P@ssW0rd", "difftest","test1");
        // 构造目标数据源MetaConfig对象
        MetaConfig metaConfigTarget = new MetaConfig("cluster.lauvinson.com", 30306, "root", "P@ssW0rd", "difftest","test2");
        // 构造CompareParam对象
        CompareParam compareParam = new CompareParam(metaConfig, metaConfigTarget);
        ConnectionForCompare connection = queryService.createStatementForCompare(compareParam.getSource(), compareParam.getTarget());
        compareStructure(connection, compareParam);
        compareData(connection, compareParam);
        queryService.close(connection.getSource(), connection.getTarget());
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime) + "ms");
        System.exit(1);
    }

    // 对比两个数据库的表结构
    private void compareStructure(ConnectionForCompare stat, CompareParam compareParam){
        // 获取两个表的索引
        try {
            List<Index> sourceIndex = queryService.getIndices(stat.getSource().createStatement(), compareParam.getSource().getTable());
            System.out.println("=========源表索引===========");
            for (Index index : sourceIndex) {
                if (index.getName().equals("PRIMARY")) {
                    System.out.println("主键：" + index.getName() + "，索引列：" + index.getColumn() + "，索引类型：" + index.getType());
                }
            }
            System.out.println("==========================");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            List<Index> targetIndex = queryService.getIndices(stat.getTarget().createStatement(), compareParam.getTarget().getTable());
            System.out.println("=========目标表索引==========");
            for (Index index : targetIndex) {
                if (index.getName().equals("PRIMARY")) {
                    System.out.println("主键：" + index.getName() + "，索引列：" + index.getColumn() + "，索引类型：" + index.getType());
                }
            }
            System.out.println("==========================");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量从ResultSet中取batchSize条数据
     * @param rs ResultSet
     * @param hasNext 是否还有下一条数据
     * @param batchSize 批量大小
     * @param result 批量数据存放map
     * @return hasNext 是否还有下一条数据
     * @throws SQLException SQL异常
     */
    private boolean batchData(ResultSet rs, boolean hasNext, int batchSize, Map<Object, Object> result) throws SQLException {
        if (hasNext) { // 如果还有下一条数据
            result.clear(); // 清空存放批量数据的map
            while (hasNext && batchSize > 0) { // 如果还有下一条数据，并且批量大小大于0(数据量不能超过batchSize)
                result.put(rs.getObject("id"), rs.getObject("id")); // 将id存放到map中
                hasNext = rs.next(); // 是否还有下一条数据
                batchSize--; // 批量大小减1
            }
        }
        return hasNext; // 是否还有下一条数据
    }


    // 对比两个数据库的表数据
    private void compareData(ConnectionForCompare stat, CompareParam compareParam) {
        ResultSet rsSource = null, rsTarget = null; // 源数据和目标数据的ResultSet
        List<Object> diffIds = new ArrayList<>(); // 不同的数据id
        try {
            int batchSize = 1000; // 批量大小
            float batched = 0f; // 已经批量的数据条数
            int countSource = queryService.count(stat.getSource().createStatement(), compareParam.getSource().getTable()); // 记录比对的数据条数
            Statement statSource = stat.getSource().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY); // 创建源数据的Statement
            Statement statTarget = stat.getTarget().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY); // 创建目标数据的Statement
            statSource.setFetchSize(10); // 设置源数据的Statement的fetchSize
            statTarget.setFetchSize(10); // 设置目标数据的Statement的fetchSize
            rsSource = queryService.query(statSource, compareParam.getSource().getTable()); // 获取源数据的ResultSet
            rsTarget = queryService.query(statTarget, compareParam.getTarget().getTable()); // 获取目标数据的ResultSet
            Map<Object, Object> sourceMap = new HashMap<>(), targetMap = new HashMap<>();  // 存放每一批的源数据和目标数据
            boolean sourceHasNext = rsSource.next(), targetHasNext = rsTarget.next(); // 源数据和目标数据是否还有下一条数据
            while (sourceHasNext || targetHasNext) { // 当源数据或者目标数据有下一条数据时
                sourceHasNext = batchData(rsSource, sourceHasNext, batchSize, sourceMap); // 批量从ResultSet中取1000条源数据
                targetHasNext = batchData(rsTarget, targetHasNext, batchSize, targetMap); // 批量从ResultSet中取1000条目标数据

                batched += sourceMap.size(); // 已经批量的数据条数加上当前批量的数据条数
                sourceMap.keySet().removeAll(targetMap.keySet()); // 去除相同的数据
                if (!CollectionUtils.isEmpty(sourceMap)) { // 如果源数据有不同的数据
                    diffIds.addAll(sourceMap.values()); // 将不同的数据id添加到diffIds中
                }
                System.out.println("对比进度:" + (batched/countSource*100) + "%");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            queryService.close(rsSource, rsTarget);
        }
        System.out.println("=========差异数据==========");
        for (Object diffId : diffIds) {
            System.out.println(diffId);
        }
        System.out.println("==========================");
//        Map<Integer, BatchMD5> targetMD5;
//        try {
//            targetMD5 = queryService.groupMD5(stat.getTarget().createStatement(), compareParam.getTarget().getTable(), 1000);
//        } catch (SQLException e) {
//            e.printStackTrace();
//            return;
//        }
//        sourceMD5.forEach((k, v) -> {
//            System.out.println("批次：" + k + "，最小id：" + v.getMinId() + "，最大id：" + v.getMaxId() + "，数据量：" + v.getRowCount() + "，md5值：" + v.getMd5Value());
//        });
//        targetMD5.forEach((k, v) -> {
//            System.out.println("批次：" + k + "，最小id：" + v.getMinId() + "，最大id：" + v.getMaxId() + "，数据量：" + v.getRowCount() + "，md5值：" + v.getMd5Value());
//        });
    }

}
