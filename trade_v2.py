# 导入函数库
from __future__ import division      #除数可以显示为float
import jqdata               #导入聚宽函数库
from io import StringIO, BytesIO

import numpy as np
import pandas as pd

from datetime import timedelta,date,datetime

import random

import talib

import statsmodels.api as sm
from pykalman import KalmanFilter

import warnings
warnings.filterwarnings("ignore")

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('600104.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    log.set_level('order', 'error')
    
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）

    # 开盘前运行

    # 打印数据，供分析使用
    # 打印当天的成交记录，并呈现胜率情况
    run_daily(after_market_log_print, time='after_close', reference_security='000300.XSHG')
    
    
    run_daily(rebalance, time='14:59', reference_security='000300.XSHG')

    # run_daily(rebalance, time='13:59', reference_security='000300.XSHG')
    # run_daily(rebalance, time='10:59', reference_security='000300.XSHG')
    # run_daily(rebalance, time='9:59', reference_security='000300.XSHG')

    # run_daily(get_operate_list,time='14:58', reference_security='000300.XSHG')
    
    g.buy_list = []
    g.sell_list = []
    
    
def get_operate_list(context):
    g.buy_list,g.sell_list = get_list(context)
    log.info('原始卖出序列为：%s'%(str(g.sell_list)))
    log.info('原始买入序列为：%s'%(str(g.buy_list)))


    
# 打印当天的成交记录，并呈现胜率情况
def after_market_log_print(context):
    #得到当天所有成交记录
    log.info('$$持仓总价值：%s' %(str(context.portfolio.positions_value)))
    i = 1
    for sec in context.portfolio.positions.keys():
        log.info('$$%d、 持仓：%s，持仓天数：%d，盈亏情况： %f，价值：%f' %(i, \
                str(context.portfolio.positions[sec].security), \
                int((context.current_dt - context.portfolio.positions[sec].init_time).days), \
                float((context.portfolio.positions[sec].price - context.portfolio.positions[sec].avg_cost)/context.portfolio.positions[sec].avg_cost), \
                float(context.portfolio.positions[sec].value) 
                ))
        i += 1

    log.info('$$当日订单信息')
    orders = get_orders()
    for _order in orders.values():
        
        if _order.action == "open":
            log.info("当天买入，股票%s，价格：%f"%(str(_order.security),float(_order.price)))

        if _order.action == "close":
            log.info("当天卖出，股票%s，价格：%f"%(str(_order.security),float(_order.price)))

def rebalance(context):

    # buy_list,sell_list = get_list(context)
    buy_list,sell_list = get_list(context)
    
    print(buy_list,sell_list)
    # sell_list = set(sell_list)&set(context.portfolio.positions.keys())
    log.info('卖出序列为：%s'%(str(sell_list)))
    
    for sec in sell_list:
        if sec in context.portfolio.positions.keys():
            order_target_value(sec, 0)

    
    # buy_list = set(buy_list)|set(context.portfolio.positions.keys())
    # print buy_list
    log.info('买入序列为：%s'%(str(buy_list)))
    for sec in buy_list:
        if sec not in  context.portfolio.positions.keys():
            order_target_value(sec, context.portfolio.total_value/len(buy_list))
            # order_target_value(sec, context.portfolio.total_value/5)

    g.buy_list = []
    g.sell_list = []    
    
def get_list(context):

    buy_list = []
    sell_list = []


    
    ncount = 30
    # 上一天的时间
    end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
        
    # 读取csv文件
    body = read_file("yield_pair_test.csv")
    pairs_info = pd.read_csv(BytesIO(body),index_col=0)
    
    # 循环进行验证
    for index,row in pairs_info.iterrows():
        sec1 = '000300.XSHG'
        sec2 = row['sec_code']
        
        Price_pd_P = get_price(sec1, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "none")['close']
        Price_pd_Q = get_price(sec2, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "none")['close']


        current_data = get_current_data()
        P_now = current_data[sec1].last_price
        Q_now = current_data[sec2].last_price
    
        P_yield = P_now/Price_pd_P[-1]
        Q_yield = Q_now/Price_pd_Q[-1]


    
        # 数据表中的结构数据
        beta = row['beta']
        alpha = row['alpha']
    
        gap = Q_yield - ( beta*P_yield + alpha ) 
    
     
        # buy_point_pd  = gap < mean - 0.5*std_deviation
        # sell_point_pd = gap > mean + 0.5*std_deviation
        mean = row['mean']
        std_deviation = row['std']
        
        if gap < (mean - 0.5*std_deviation):
            log.info("符合买入条件，加入买入列表：%s"%(str(sec2)))

            buy_list.append(sec2)
        elif gap > (mean + 0.5*std_deviation):

            log.info("符合卖出条件，加入卖出列表：%s"%(str(sec2)))
            sell_list.append(sec2)
        # 调试用
        log.info('调试数据开始')
        log.info('当天价格P:%f'%float(P_now))
        log.info('前一天价格P:%f'%float(Price_pd_P[-1]))
        log.info('当天价格Q:%f'%float(Q_now))
        log.info('前一天价格Q:%f'%float(Price_pd_Q[-1]))
    
        log.info('P收益率为:%f'%float(P_yield))
        log.info('Q收益率为:%f'%float(Q_yield))
    
        
        log.info('Q_yield:%f'%float(Q_yield))
        log.info('P_yield:%f'%float(P_yield))
        log.info('gap:%f'%float(gap))
    
    
        

    return buy_list,sell_list



# def buy(context):
    
#     buy_list = get_buy_list(context)
#     buy_list = set(buy_list)|set(context.portfolio.positions.keys())
#     # print buy_list
#     for sec in buy_list:
#         order_target_value(sec, context.portfolio.total_value/len(buy_list))
#         # order_target_value(sec, context.portfolio.total_value*3/g.pairs_info.shape[0])


# def get_buy_list(context):

#     buy_list = []
    
#     # 仅模拟一支股票
#     # '000300.XSHG'
#     # '601939.XSHG'
    
    
#     sec1 = '000300.XSHG'
#     sec2 = '601939.XSHG'
    
#     ncount = 30
#     # 上一天的时间
#     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
    
#     Price_pd_P = get_price(sec1, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     Price_pd_Q = get_price(sec2, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']


#     current_data = get_current_data()
#     P_now = current_data[sec1].last_price
#     Q_now = current_data[sec2].last_price
    
#     P_yield = P_now/Price_pd_P[-1]
#     Q_yield = Q_now/Price_pd_Q[-1]


    
#     # 研究模块中求出结果
#     beta = 0.5222431884670595 
#     alpha = 0.522233585709004
    
    

#     gap = Q_yield - ( beta*P_yield + alpha ) 
    
    
#     mean = -0.04350904785720012
#     std_deviation = 0.012933218296693744
    
#     if gap > (mean + 1*std_deviation):
#         buy_list.append(sec2)
#     else:
#         buy_list = []
        
#     return buy_list

#     # for index,row in g.pairs_info.iterrows():

#     #     # 1. 获取数据
#     #     # 获取上一天的时间
#     #     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
#     #     # 获取的数据量
#     #     ncount = 20
        
#     #     # # 获取昨天的收盘价，进行对比用
#     #     Price_pd_P = get_price(row['p1'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     #     Price_pd_Q = get_price(row['p2'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
        
        
#     #     # 确定3个时间点的价格
#     #     # 前两天的收盘价
#     #     P0 = Price_pd_P[-2]
#     #     P1 = Price_pd_P[-1]
        
        
#     #     Q0 = Price_pd_Q[-2]
#     #     Q1 = Price_pd_Q[-1]
        
#     #     # 当前的价格
#     #     current_data = get_current_data()
#     #     P2 = current_data[row['p1']].last_price
#     #     Q2 = current_data[row['p2']].last_price
        
#     #     #2. 判断目前的状态
#     #     # 先判断是否是卖出状态，如果是卖出状态，则直接不用判断买入状态
#     #     # 判断依据是当前价格，前1天的数据
#     #     res_diff_2 = (Q2 - Q1) - row['beta'] * (P2 - P1)
#     #     if res_diff_2 > 1.2* row['sigma']:
#     #     # if 0:
#     #         continue
#     #     else:
#     #         # 如果是非卖出状态，再判断是否是买入状态
#     #         # 判断依据是前1天价格，前2天价格，判断是否偏离
#     #         # 接着判断偏离状态下是否是Q价格在回归
#     #         res_diff_1 = (Q1 - Q0) - row['beta'] * (P1 - P0)
            
#     #         if res_diff_1 < -0.5* row['sigma'] :
                
#     #             # P价格变化比例
#     #             Delta_P = (P2 - P1)/abs(P1 - P0)
#     #             # Q价格变化比例
#     #             Delta_Q = (Q2 - Q1)/abs(Q1 - Q0)
                
#     #             # if (Delta_P >= 1) and (Delta_Q <= 1):
#     #             # if Delta_P > 1.5 * Delta_Q:
#     #             if 1:
#     #                 buy_list.append(row['p2'])
#     #             else:
#     #                 continue
#     #         else:
#     #             continue
#     #     # 日志输出，调试检测使用
#     #     log.info("P价格为%f,%f,%f"%(P0,P1,P2))
#     #     log.info("Q价格为%f,%f,%f"%(Q0,Q1,Q2))
#     #     log.info("res_diff_1为%f"%float((Q1 - Q0) - row['beta'] * (P1 - P0)))
#     #     log.info("res_diff_2为%f"%float((Q2 - Q1) - row['beta'] * (P2 - P1)))
#     #     log.info("sigma为%f"%float(row['sigma']))
#     #     log.info("Delta_P为%f"%float((P2 - P1)/(P1 - P0)))
#     #     log.info("Delta_Q为%f"%float((Q2 - Q1)/(Q1 - Q0)))



    
#     # # 3、进行操作

#     # log.info("buy_list为%s"%buy_list)

    


# def sell(context):   
#     sell_list = get_sell_list(context)
#     sell_list = set(sell_list)&set(context.portfolio.positions.keys())
#     for sec in sell_list:
#         order_target_value(sec, 0)

# def get_sell_list(context):
    
#     sell_list = []

#     # 仅模拟一支股票
#     # '000300.XSHG'
#     # '601939.XSHG'
    
    
#     sec1 = '000300.XSHG'
#     sec2 = '601939.XSHG'
    
#     ncount = 30
#     # 上一天的时间
#     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
    
#     Price_pd_P = get_price(sec1, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     Price_pd_Q = get_price(sec2, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']


#     current_data = get_current_data()
#     P_now = current_data[sec1].last_price
#     Q_now = current_data[sec2].last_price
    
#     P_yield = P_now/Price_pd_P[-1]
#     Q_yield = Q_now/Price_pd_Q[-1]


    
#     # 研究模块中求出结果
#     beta = 0.5222431884670595 
#     alpha = 0.522233585709004
    
    

#     gap = Q_yield - ( beta*P_yield + alpha ) 
    
    
#     mean = -0.04350904785720012
#     std_deviation = 0.012933218296693744
    
#     if gap < (mean - 0.5*std_deviation):
#         sell_list.append(sec2)
#     else:
#         sell_list = []
        
#     return sell_list
#     # for index,row in g.pairs_info.iterrows():

#     #     # 1. 获取数据
#     #     # 获取上一天的时间
#     #     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
#     #     # 获取的数据量
#     #     ncount = 20
        
#     #     # # 获取昨天的收盘价，进行对比用
#     #     Price_pd_P = get_price(row['p1'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     #     Price_pd_Q = get_price(row['p2'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
        
        
#     #     # 确定3个时间点的价格
#     #     # 前两天的收盘价
#     #     P0 = Price_pd_P[-2]
#     #     P1 = Price_pd_P[-1]
        
        
#     #     Q0 = Price_pd_Q[-2]
#     #     Q1 = Price_pd_Q[-1]
        
#     #     # 当前的价格
#     #     current_data = get_current_data()
#     #     P2 = current_data[row['p1']].last_price
#     #     Q2 = current_data[row['p2']].last_price
        
#     #     #2. 判断目前的状态
#     #     # 先判断是否是卖出状态，如果是卖出状态，则直接不用判断买入状态
#     #     # 判断依据是当前价格，前1天的数据
#     #     res_diff_2 = (Q2 - Q1) - row['beta'] * (P2 - P1)
#     #     # if res_diff_2 > -0.2*row['sigma']:
#     #     if res_diff_2 >  0.5* row['sigma']:

#     #         sell_list.append(row['p2'])
            
#     # return sell_list
