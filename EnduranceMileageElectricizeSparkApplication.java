package com.sziov.gacnev.dwd;

import com.sziov.gacnev.builder.SparkBuilder;
import com.sziov.gacnev.config.JDBCConfig;
import com.sziov.gacnev.context.ApplicationContext;
import com.sziov.gacnev.utils.JDBCUtil;
import lombok.Data;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class EnduranceMileageElectricizeSparkApplication {
    public static void main(String[] args) {
        String dateStr = args[0];
        ApplicationContext applicationContext = ApplicationContext.initContext(args);
        JDBCConfig mysqlConfig = applicationContext.getApplicationConfig().getJdbcConfig();

        Map<String, List<SocCal>> socCalMap = getSocCalMap(dateStr, mysqlConfig);
        if (socCalMap == null || socCalMap.size() == 0) {
            System.out.println("------------规则未缓存，不计算-------------");
            System.exit(0);
        }

        /**
         * 用不上的
         */
//        List<Map<String, Object>> vehicleList = JDBCUtil.executeQuery(jdbcConfig, "select distinct * from vms_center.dw_dim_vehicle;");
//        if (CollectionUtils.isEmpty(vehicleList)) {
//            System.out.println("查询车辆为空");
//            return;
//        }
//        //去重
//        System.out.println("去重前车辆：" + vehicleList.size());
//        List<DwDimVehiclePo> newVehicleList = vehicleList.stream().collect(
//                Collectors.collectingAndThen(
//                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(DwDimVehiclePo::getVin))), ArrayList::new)
//        );
//        System.out.println("去重后车辆：" + newVehicleList.size());

        /**
         * 数据源
         */
        SparkSession spark = SparkBuilder.build();
        String sourcePath = "/user/x5l/hive/dw_vms.db/charging_statistics/sample_date="+dateStr;
        Dataset<Row> sourceDataset = spark.read().parquet(sourcePath);
        KeyValueGroupedDataset<String, Row> keyValueGroupedDataset = sourceDataset.groupByKey((MapFunction<Row, String>) row -> row.getAs("vin"), Encoders.STRING());
        keyValueGroupedDataset.flatMapGroups(new FlatMapGroupsFunction<String, Row, EnduranceMileageElectricizePo>() {
            @Override
            public Iterator<EnduranceMileageElectricizePo> call(String s, Iterator<Row> iterator) throws Exception {
                return null;
            }
        }, Encoders.bean(EnduranceMileageElectricizePo.class));
    }
    

    /**
     * 根据不同车型,不同版本 计算真实起止SOC 和 SOH  和额定容量差
     *
     * @return 一个map map中记录了起止SOC 和SOH 和 额定容量差
     */
    private static Boolean judgeInterval(String symbol, Double interval, Double mileage) {

        switch (symbol){
            case ">":
                if (mileage > interval) {
                    return true;
                } else {
                    return false;
                }
            case ">=":
                if (mileage >= interval) {
                    return true;
                } else {
                    return false;
                }
            case "<":
                if (mileage < interval) {
                    return true;
                } else {
                    return false;
                }
            case "<=":
                if (mileage <= interval) {
                    return true;
                } else {
                    return false;
                }
            case "":
                return true;
            default:
                return null;
        }

    }
    public static SocCal getSocCal(Long startTime, Double mileage, List<SocCal> vinSocCalList) {
        for (SocCal socCal : vinSocCalList) {
            if(socCal.getStartTime() == null){
                continue;
            }
            long soccalStartTime = socCal.getStartTime().getTime();
            if (startTime >= soccalStartTime && (socCal.getEndTime() == null || startTime <= socCal.getEndTime().getTime())) {
                List<SocRuleDetail> socRuleDetails = socCal.getSocRuleDetails();
                if (socRuleDetails == null) {
                    System.out.println("找不到对应soc规则,socRuleDetails为Null:" + socCal.getVin() + "|" + socCal.getBatteryType());
                    return socCal;
                }
                SocCal socCalResult = new EnduranceMileageElectricizeSparkApplication().new SocCal();
                socCalResult.setVin(socCal.getVin());
                socCalResult.setEndTime(socCal.getEndTime());
                socCalResult.setStartTime(socCal.getStartTime());
                socCalResult.setBatteryType(socCal.getBatteryType());
                socCalResult.setElectricCapacity(socCal.getElectricCapacity());
                socCalResult.setElectricQuantity(socCal.getElectricQuantity());
                for (SocRuleDetail socRuleDetail : socRuleDetails) {
                    String intervalMileage = socRuleDetail.getIntervalMileage();
                    String[] intervalMileageSplit = intervalMileage.split(",", -1);
                    if (intervalMileageSplit.length != 4) {
                        System.out.println(intervalMileage+"里程判断规则切分异常");
                        continue;
                    }
                    String perfixSymbol  = intervalMileageSplit[0];
                    String suffixSymbol = intervalMileageSplit[2];

                    //例如[0,10000),前判断>=0
                    Boolean prefixJudge = null;
                    if ("".equals(perfixSymbol)) {
                        prefixJudge = true;
                    } else {
                        try {
                            Double perfixInterval = Double.valueOf(intervalMileageSplit[1]);
                            prefixJudge = judgeInterval(perfixSymbol, perfixInterval, mileage);
                        } catch (NumberFormatException e) {
                            System.out.println(intervalMileage+"里程判断规则符号异常");
                        }
                    }
                    Boolean suffixJudge = null;
                    //例如[0,10000),后判断<10000
                    if ("".equals(suffixSymbol)) {
                        suffixJudge = true;
                    }else {
                        try {
                            Double suffixInterval = Double.valueOf(intervalMileageSplit[3]);
                            suffixJudge = judgeInterval(suffixSymbol, suffixInterval, mileage);
                        } catch (NumberFormatException e) {
                            System.out.println(intervalMileage+"里程判断规则符号异常");
                        }
                    }
                    if (prefixJudge == null || suffixJudge == null) {
                        System.out.println(intervalMileage+"里程判断规则符号异常");
                        continue;
                    }
                    if (prefixJudge && suffixJudge) {
                        List<SocRuleDetail> list = new ArrayList<>();
                        list.add(socRuleDetail);
                        socCalResult.setSocRuleDetails(list);
                    }
                }
                return socCalResult;
            }
        }
        return null;
    }

    private static SocCal  getResultMapNew(String vin,Long startTime, Double mileage , List<SocCal> vinSocCalList) {


        SocCal socCal = null;
        if (vinSocCalList != null) {
            socCal = getSocCal(startTime, mileage, vinSocCalList);
        } else {
            System.out.println(vin + "找不到对应的电池履历");
        }
        return socCal;
    }

    /**
     * @描述: 续航里程表-充电记录版实体类
     * @公司:
     * @作者: 商建利
     * @版本: 1.0.0
     * @日期: 2019-09-24 18:04:51
     */
    @Data
    class EnduranceMileageElectricizePo implements Serializable {
        private String id;

        /**
         * vin码
         */
        private String vin;

        /**
         * 车型
         */
        private String vehicleType;

        /**
         * 上次充电结束时间
         */
        private Date endTime;

        /**
         * 变更前soc 上条充电记录时的结束soc
         */
        private Integer altFrontSoc;

        /**
         * 变更后soc 此次充电记录时的开始soc
         */
        private Integer altRearSoc;

        /**
         * 变更前里程数 上条充电记录时的里程数
         */
        private Double altFrontMileage;

        /**
         * 变更后里程数 此次充电记录时的里程数
         */
        private Double altRearMileage;

        /**
         * 预估里程
         */
        private Double estimateMileage;

        /**
         * 上一条充电记录id
         */
        private String lastTimeRecordId;

        /**
         * 此次充电记录id
         */
        private String thisTimeRecordId;

        /**
         * 百公里电耗
         */
        private Double electricQuantity100km;

        /**
         * 本次充电开始时间
         */
        private Date startTime;

        /**
         * 行使里程
         */
        private Double runMileage;

        /**
         * 显示SOC差值
         */
        private Integer showDifference;

        /**
         * 真实SOC差值
         */
        private Double realDifference;

        /**
         * 真实开始SOC
         */
        private Double realStartSoc;

        /**
         * 真实结束SOC
         */
        private Double realEndSoc;

        /**
         * 实际耗电量
         */
        private Double powerConsumption;

        /**
         * 平均百公里电耗
         */
        private Double avgElectricQuantity100km;

        /**
         * 原mysql的自增id，今废弃
         */
        private Integer sysId = 0;

        private static final long serialVersionUID = 1L;
    }

    @Data
    class BatteryConfCal implements Serializable {
        private String vin;
        private String vehicleType;
        private String batteryType;
        private Date createTime;
        private Date updateTime;
        private String configId;
        private Double electricCapacity;
        private Double electricQuantity;

    }

    @Data
    class SocRuleDetail implements Serializable {
        private Integer id;
        private String intervalMileage;
        private String socRule;
        private Integer parentId;
    }

    @Data
    class SocCal implements Serializable {

        private String vin;
        private String batteryType;
        private Date startTime;
        private Date endTime;
        //电容
        private Double electricCapacity;
        //电量
        private Double electricQuantity;
        //规则
        private List<SocRuleDetail> socRuleDetails;

    }


    public static Map<String, List<SocCal>> getSocCalMap(String dateStr, JDBCConfig mysqlConfig){
        String sql = "SELECT id,interval_mileage as intervalMileage,soc_rule as socRule ,parent_id as parentId from vehicle_soc_rule_detail where `disable`='0'";
        List<SocRuleDetail> socRuleDetails = JDBCUtil.executeQuery(mysqlConfig, sql, SocRuleDetail.class);
        Map<Integer, List<SocRuleDetail>> socRuleDetailMap = new HashMap<>();
        socRuleDetails.forEach(socRuleDetail -> {
            if (socRuleDetailMap.get(socRuleDetail.getParentId()) == null) {
                List<SocRuleDetail> socRuleDetailList = new ArrayList<>();
                socRuleDetailList.add(socRuleDetail);
                socRuleDetailMap.put(socRuleDetail.getParentId(), socRuleDetailList);
            }else{
                socRuleDetailMap.get(socRuleDetail.getParentId()).add(socRuleDetail);
            }
        });

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String startDate = dateStr + " 00:00:00";
        String endDate = dateStr + " 23:59:59";
        sql = "SELECT " +
                " a.vin as vin, " +
                " a.battery_type as batteryType, " +
                " a.vehicle_type as vehicleType, " +
                " a.create_time as createTime, " +
                " a.update_time as updateTime, " +
                " b.id as configId, " +
                " b.electric_capacity as electricCapacity, " +
                " b.electric_quantity as electricQuantity" +
                " FROM " +
                " ( " +
                "  SELECT " +
                "   vin, " +
                "   vehicle_type, " +
                "   battery_type, " +
                "   create_time, " +
                "   update_time " +
                "  FROM " +
                "   vehicle_battery_type_history a " +
                "  WHERE " +
                "   ( " +
                "    create_time >= '" + startDate + "' " +
                "    AND create_time <= '" + endDate + "' " +
                "   ) " +
                "  OR ( " +
                "   update_time >= '" + startDate + "' " +
                "   AND update_time <= '" + endDate + "' " +
                "  ) " +
                "  OR ( " +
                "   create_time < '" + startDate + "' " +
                "   AND ( " +
                "    update_time > '" + endDate + "' " +
                "    OR update_time IS NULL " +
                "   ) " +
                "  ) " +
                " ) a " +
                "LEFT JOIN vehicle_battery_type_conf b ON CONCAT_WS('|',a.battery_type,a.vehicle_type) = CONCAT_WS('|',b.battery_type,b.vehicle_type) " +
                "WHERE " +
                " b.`disable` = '0'";
        List<BatteryConfCal> batteryConfCalList = JDBCUtil.executeQuery(mysqlConfig, sql, BatteryConfCal.class);

        Map<String, Map> defaultBatteryConfMap = new HashMap<>();
        sql = "SELECT vehicle_type as vehicleType,battery_type as batteryType FROM `vehicle_battery_type_conf` WHERE default_data = '1' and `disable` = '0' ;";
        List<Map<String, Object>> tmpList = JDBCUtil.executeQuery(mysqlConfig, sql);
        for(Map<String, Object> map:tmpList){
            String vehicleType = String.valueOf(map.get("vehicleType"));
            String batteryType = String.valueOf(map.get("batteryType"));
            Map<String, String> tmpMap = new HashMap<>();
            tmpMap.put("vehicleType", vehicleType);
            tmpMap.put("batteryType", batteryType);
            defaultBatteryConfMap.put(vehicleType, tmpMap);
        }

        Map<String, Map> defaultSocRuleMap = new HashMap<>();
        sql = "SELECT " +
                " id as id, " +
                " software_version as softwareVersion, " +
                " hardware_version as hardwareVersion, " +
                " battery_type as batteryType " +
                " FROM " +
                " vehicle_soc_rule b " +
                "WHERE " +
                " b.ecu_name = 'BMS' " +
                "AND b.`disable` = '0' " +
                "AND b.default_data = '1'";
        tmpList = JDBCUtil.executeQuery(mysqlConfig, sql);
        for(Map<String, Object> map:tmpList){
            Map<String, Object> tmpMap = new HashMap<>();
            Integer id = Integer.valueOf(String.valueOf(map.get("id")));
            String batteryType = String.valueOf(map.get("batteryType"));
            String hardwareVersion = String.valueOf(map.get("hardwareVersion"));
            String softwareVersion = String.valueOf(map.get("softwareVersion"));
            List<SocRuleDetail> socRuleDetailList = socRuleDetailMap.get(id);
            tmpMap.put("batteryType", batteryType);
            tmpMap.put("id", id);
            tmpMap.put("hardwareVersion", hardwareVersion);
            tmpMap.put("softwareVersion", softwareVersion);
            tmpMap.put("socRuleDetails", socRuleDetailList);
            defaultSocRuleMap.put(batteryType, tmpMap);
        }

        List<SocCal> socCals = new ArrayList<>();
        if (batteryConfCalList == null || batteryConfCalList == null || defaultBatteryConfMap == null) {
        }else {
            for (BatteryConfCal batteryConfCal : batteryConfCalList) {
                if (batteryConfCal.getCreateTime()==null||(batteryConfCal.getUpdateTime()!=null&&batteryConfCal.getCreateTime().getTime() > batteryConfCal.getUpdateTime().getTime())) {
                    continue;
                }
                String key = batteryConfCal.getVin() + "-" + batteryConfCal.getBatteryType();
                //构造对象
                SocCal socCal = new EnduranceMileageElectricizeSparkApplication().new SocCal();
                socCal.setBatteryType(batteryConfCal.getBatteryType());
                socCal.setVin(batteryConfCal.getVin());
                //如果电池配置获取不到，则根据车型去默认值里找电池配置，如果有则赋值
                if (batteryConfCal.getConfigId() == null) {
                    Map tmpMap = defaultSocRuleMap.get(batteryConfCal.getVehicleType());
                    if (defaultBatteryConfMap != null) {
                        try {
                            socCal.setElectricCapacity((Double) tmpMap.get("electricCapacity"));
                            socCal.setElectricQuantity((Double) tmpMap.get("electricQuantity"));
                        } catch (Exception e) {
                            System.out.println("key:" + key + "获取额定电容电量失败");
                        }
                    } else {
                        System.out.println(batteryConfCal.getVehicleType() + "车型默认额定电容电量为空");
                    }
                } else {
                    socCal.setElectricCapacity(batteryConfCal.getElectricCapacity());
                    socCal.setElectricQuantity(batteryConfCal.getElectricQuantity());
                }
                //根据key:vin-电池类型，获取List<SocRuleCal>,一个车型-电池类型一个相应默认的soc计算规则
                Map defaultSocRule = defaultSocRuleMap.get(batteryConfCal.getBatteryType());
                //采用默认电池类型配置，开始时间结束时间取Soc规则的开始时间结束时间
                socCal.setStartTime(batteryConfCal.getCreateTime());
                socCal.setEndTime(batteryConfCal.getUpdateTime());
                if (defaultSocRule == null) {

                    System.out.println(batteryConfCal.getBatteryType() + "没有默认规则");
                } else {
                    socCal.setSocRuleDetails((List<SocRuleDetail>) defaultSocRule.get("socRuleDetails"));
                }
                socCals.add(socCal);
            }
        }

        Map<String, List<SocCal>> socCalMap = new HashMap<>();
        for (SocCal socCal : socCals) {
            String vin = socCal.getVin();
            if (socCalMap.get(vin) == null) {
                List<SocCal> vinSocCalList = new ArrayList<>();
                vinSocCalList.add(socCal);
                socCalMap.put(vin, vinSocCalList);
            } else {
                socCalMap.get(vin).add(socCal);
            }
        }
        return socCalMap;
    }

}
