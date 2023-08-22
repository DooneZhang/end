package com.sziov.gacnev.executor;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.sziov.battery.life.entity.po.SocCal;
import com.sziov.common.utils.DateUtils;
import com.sziov.common.utils.StringUtils;
import com.sziov.gacnev.batterydao.BigDataTaskInfoMapper;
import com.sziov.gacnev.batterydao.ChargingStatisticsMapper;
import com.sziov.gacnev.batterydao.EnduranceMileageElectricizeMapper;
import com.sziov.gacnev.batterydao.OperateMonthReportConfMapper;
import com.sziov.gacnev.dao.DwDimVehicleMapper;
import com.sziov.gacnev.entity.dto.AvgElectricQuantity100km;
import com.sziov.gacnev.entity.dto.EnduranceMileageMaxDateDto;
import com.sziov.gacnev.entity.po.*;
import com.sziov.gacnev.utils.DatesUtil;
import com.sziov.gacnev.utils.MathFormulaUtil;
import com.sziov.gacnev.utils.Sql2SocCalUtils;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import com.xxl.job.core.log.XxlJobLogger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.sziov.common.utils.DateUtils.PATTERN_YYYYMMDD;

/**
 * @描述: 根据充电统计计算续航里程
 * @公司:
 * @作者: 汪胜财
 * @版本: 1.0.0
 * @日期: 2019-09-24 13:51:58
 */
@JobHandler(value = "drivingMileageV2JobHander")
@Component
@Slf4j
public class DrivingMileageV2JobHander extends IJobHandler {

    @Autowired
    private DwDimVehicleMapper dwDimVehicleMapper;

    @Autowired
    private ChargingStatisticsMapper chargingStatisticsMapper;

    @Autowired
    private EnduranceMileageElectricizeMapper enduranceMileageElectricizeMapper;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private OperateMonthReportConfMapper operateMonthReportConfMapper;

    @Autowired
    private BigDataTaskInfoMapper bigDataTaskInfoMapper;

    @Autowired
    private ThreadPoolTaskExecutor executor;

    @Override
    public ReturnT<String> execute(String s) throws Exception {

        Date yesterDate = DatesUtil.getBeginDayOfYesterday();
        //判断充电是否执行完毕
        if (!this.getTaskState(yesterDate, s)) {
            XxlJobLogger.log(DateUtils.formatDate(yesterDate, DateUtils.PATTERN_DATE) + "充电数据大数据还未计算完毕");
            return FAIL;
        }
        //判断规则是否缓存
        Map<String, List<SocCal>> socCalMap = Sql2SocCalUtils.getSocCalMap(yesterDate, redisTemplate);
        if (socCalMap == null || socCalMap.size() == 0) {
            XxlJobLogger.log("------------规则未缓存，不计算-------------");
            return FAIL;
        }
        XxlJobLogger.log("------------续航里程计算定时任务执行开始-start-------------");
        //获取所有车的预估里程记录表的最大时间
        List<EnduranceMileageMaxDateDto> maxDateDtos = enduranceMileageElectricizeMapper.selectMaxDate();
        XxlJobLogger.log("获取所有车辆计算到哪一次的充电时间完毕");
        Map<String, List<EnduranceMileageMaxDateDto>> collect1 = maxDateDtos.stream().collect(Collectors.groupingBy(EnduranceMileageMaxDateDto::getVin));
        DwDimVehiclePoExample example = new DwDimVehiclePoExample();
        List<DwDimVehiclePo> vehicleList = dwDimVehicleMapper.selectByExample(example);
        XxlJobLogger.log("获取所有车辆信息完毕");
        List<EnduranceMileageElectricizePo> poList = new ArrayList<>();
        try {
            List<Future<List<EnduranceMileageElectricizePo>>> submitList = new ArrayList<>();
            for (DwDimVehiclePo dwDimVehiclePo : vehicleList) {
                Future<List<EnduranceMileageElectricizePo>> submit = executor.submit(() -> {
                    Map<String, Object> params = new HashMap<>();
                    params.put("vin", dwDimVehiclePo.getVin());
                    if (CollectionUtils.isNotEmpty(collect1.get(dwDimVehiclePo.getVin()))) {
                        params.put("time", collect1.get(dwDimVehiclePo.getVin()).get(0).getMaxDate());
                    }
                    //params.put("vehicleType", vehicleTypeList);
                    List<ChargingStatisticsPo> chargingStatisticsPos = chargingStatisticsMapper.selectDataByVinAndTime(params);
                    List<EnduranceMileageElectricizePo> tempList = new ArrayList<>();
                    for (int i = 0; i < chargingStatisticsPos.size(); i++) {
                        ChargingStatisticsPo frontPo = chargingStatisticsPos.get(i);
                        if (i == chargingStatisticsPos.size() - 1) {
                            break;
                        }
                        ChargingStatisticsPo rearPo = chargingStatisticsPos.get(i + 1);
                        EnduranceMileageElectricizePo enduranceMileageElectricizePo = new EnduranceMileageElectricizePo();
                        enduranceMileageElectricizePo.setId(frontPo.getVin() + "_" + frontPo.getEndTime().getTime());
                        enduranceMileageElectricizePo.setVin(frontPo.getVin());
                        enduranceMileageElectricizePo.setVehicleType(frontPo.getVehicleType());
                        enduranceMileageElectricizePo.setEndTime(frontPo.getEndTime());
                        enduranceMileageElectricizePo.setAltFrontSoc(frontPo.getEndSoc());
                        enduranceMileageElectricizePo.setAltRearSoc(rearPo.getStartSoc());
                        enduranceMileageElectricizePo.setAltFrontMileage(frontPo.getMileage());
                        enduranceMileageElectricizePo.setAltRearMileage(rearPo.getMileage());
                        double mileageDifference = rearPo.getMileage() - frontPo.getMileage();
                        int socDifference = frontPo.getEndSoc() - rearPo.getStartSoc();
                        Double estimateMileage = -1.0;
                        Double totalEleQuantity100Km = -1.0;
                        Double realStartSoc = -1.0;
                        Double realEndSoc = -1.0;
                        Double powerConsumption = -1.0;
                        SocCal socCal = getResultMapNew(frontPo.getVin(), frontPo.getStartTime().getTime(), frontPo.getMileage(), socCalMap.get(frontPo.getVin()));
                        try {
                            String socRule = socCal.getSocRuleDetails().get(0).getSocRule();
                            realStartSoc = Double.valueOf(MathFormulaUtil.evaluateExpressionDouble(socRule.replace("soc", frontPo.getEndSoc().toString())));
                        } catch (Exception e) {
                        }
                        try {
                            String socRule = socCal.getSocRuleDetails().get(0).getSocRule();
                            realEndSoc = Double.valueOf(MathFormulaUtil.evaluateExpressionDouble(socRule.replace("soc", rearPo.getStartSoc().toString())));
                        } catch (Exception e) {
                        }
                        double realDifference = realStartSoc - realEndSoc;
                        try {
                            if (mileageDifference != 0 && realDifference != 0 && socCal != null) {
                                powerConsumption = realDifference / 100.0 * socCal.getElectricQuantity();
                                totalEleQuantity100Km = powerConsumption / mileageDifference;
                            }
                        } catch (Exception e) {

                        }
                        if (mileageDifference != 0 && socDifference > 0) {
                            estimateMileage = (mileageDifference / socDifference) * 100;
                        }
                        enduranceMileageElectricizePo.setEstimateMileage(Double.isNaN(estimateMileage) || Double.isNaN(estimateMileage) ? null : Double.valueOf(String.format("%.2f", estimateMileage)));
                        enduranceMileageElectricizePo.setLastTimeRecordId(frontPo.getId());
                        enduranceMileageElectricizePo.setThisTimeRecordId(rearPo.getId());
                        enduranceMileageElectricizePo.setElectricQuantity100km(Double.isNaN(totalEleQuantity100Km) || Double.isNaN(totalEleQuantity100Km) ? null : Double.valueOf(String.format("%.6f", totalEleQuantity100Km)));
                        enduranceMileageElectricizePo.setStartTime(rearPo.getStartTime());
                        enduranceMileageElectricizePo.setRunMileage(Double.isNaN(mileageDifference) || Double.isNaN(mileageDifference) ? null : Double.valueOf(String.format("%.2f", mileageDifference)));
                        enduranceMileageElectricizePo.setShowDifference(socDifference);
                        enduranceMileageElectricizePo.setRealStartSoc(realStartSoc);
                        enduranceMileageElectricizePo.setRealEndSoc(realEndSoc);
                        enduranceMileageElectricizePo.setRealDifference(realDifference);
                        enduranceMileageElectricizePo.setPowerConsumption(powerConsumption);
                        SocCal batterySocCal = Sql2SocCalUtils.getBatterySocCal(enduranceMileageElectricizePo.getStartTime().getTime(), socCalMap.get(dwDimVehiclePo.getVin()));
                        if (batterySocCal != null) {
                            Double electricQuantity100km = this.getElectricQuantity100km(dwDimVehiclePo.getVin(), enduranceMileageElectricizePo.getRealDifference(),
                                    batterySocCal.getElectricQuantity(), enduranceMileageElectricizePo.getRunMileage());
                            enduranceMileageElectricizePo.setAvgElectricQuantity100km(electricQuantity100km == null ? null : Double.valueOf(String.format("%.4f", electricQuantity100km)));
                        }
                        tempList.add(enduranceMileageElectricizePo);
                    }
                    return tempList;
                });
                submitList.add(submit);
            }
            for (Future<List<EnduranceMileageElectricizePo>> submit : submitList) {
                List<EnduranceMileageElectricizePo> tempList = submit.get();
                if (CollectionUtils.isNotEmpty(tempList)) {
                    poList.addAll(tempList);
                }
            }
            XxlJobLogger.log("开始批量新增");
            Lists.partition(poList, 1000).stream().forEach(e -> enduranceMileageElectricizeMapper.batchInsert(e));
            XxlJobLogger.log("数量" + poList.size());
            return SUCCESS;
        } catch (Exception e) {
            XxlJobLogger.log(e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取平均百公里电耗
     *
     * @return
     */
    private Double getElectricQuantity100km(String vin, Double realDifference, Double electricQuantity, Double runMileage) {
        if (realDifference == null || realDifference == 0.0 || runMileage == null || runMileage == 0.0 || electricQuantity == null) {
            return null;
        }
        //从缓存拿出上一次计算平均百公里电耗的数据
        Object avg = redisTemplate.opsForValue().get("battery:avgElectricQuantity100km:" + vin);
        //判断是否为空
        if (avg == null) {
            AvgElectricQuantity100km avgElectricQuantity100km = new AvgElectricQuantity100km();
            avgElectricQuantity100km.setVin(vin);
            avgElectricQuantity100km.setSumRealDifference(realDifference);
            avgElectricQuantity100km.setElectricQuantity(electricQuantity);
            avgElectricQuantity100km.setSumRunMileage(runMileage);
            redisTemplate.opsForValue().set("battery:avgElectricQuantity100km:" + vin, JSON.toJSONString(avgElectricQuantity100km));
            double v = realDifference / 100.0 * electricQuantity / runMileage;
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                return 0D;
            }
            return v;
        }
        AvgElectricQuantity100km avgElectricQuantity100km = JSON.parseObject(String.valueOf(avg), AvgElectricQuantity100km.class);
        avgElectricQuantity100km.setSumRealDifference(avgElectricQuantity100km.getSumRealDifference() + realDifference);
        avgElectricQuantity100km.setElectricQuantity(electricQuantity);
        avgElectricQuantity100km.setSumRunMileage(avgElectricQuantity100km.getSumRunMileage() + runMileage);
        double v = avgElectricQuantity100km.getSumRealDifference() / 100.0 * electricQuantity / avgElectricQuantity100km.getSumRunMileage();
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            return 0D;
        }
        return v;
    }

    /**
     * 根据不同车型,不同版本 计算真实起止SOC 和 SOH  和额定容量差
     *
     * @return 一个map map中记录了起止SOC 和SOH 和 额定容量差
     */
    private static SocCal getResultMapNew(String vin, Long startTime, Double mileage, List<SocCal> vinSocCalList) {


        SocCal socCal = null;
        if (vinSocCalList != null) {
            socCal = Sql2SocCalUtils.getSocCal(startTime, mileage, vinSocCalList);
        } else {
            log.error(vin + "找不到对应的电池履历");
        }
        return socCal;
    }

    /**
     * 查询大数据的里程是否计算完毕,true=执行完毕，false=还没执行完毕
     *
     * @param endTime
     * @return
     */
    private boolean getTaskState(Date endTime, String s) {
        //如果调度传了参数进来，则表示这个任务是手动执行的，就跳过校验，直接执行参数指定的日期的数据
        if (StringUtils.isNotEmpty(s)) {
            return true;
        }
        //查询大数据是否执行完毕
        BigDataTaskInfoPoExample example = new BigDataTaskInfoPoExample();
        BigDataTaskInfoPoExample.Criteria criteria = example.createCriteria();
        criteria.andDataTimeEqualTo(DateUtils.formatDate(endTime, PATTERN_YYYYMMDD));
        criteria.andDataTypeIn(Lists.newArrayList(4));//1=里程，2=电池全生命，3=电池全生命SOH，4=充电，5=轨迹
        criteria.andDorisStatusEqualTo(1);
        long count = bigDataTaskInfoMapper.countByExample(example);
        if (count == 0) {
            return false;
        }
        return true;
    }


}
