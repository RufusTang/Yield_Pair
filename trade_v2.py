# ���뺯����
from __future__ import division      #����������ʾΪfloat
import jqdata               #����ۿ�����
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

# ��ʼ���������趨��׼�ȵ�
def initialize(context):
    # �趨����300��Ϊ��׼
    set_benchmark('600104.XSHG')
    # ������̬��Ȩģʽ(��ʵ�۸�)
    set_option('use_real_price', True)
    # ������ݵ���־ log.info()
    log.info('��ʼ������ʼ������ȫ��ֻ����һ��')
    # ���˵�orderϵ��API�����ı�error����͵�log
    log.set_level('order', 'error')
    
    ### ��Ʊ����趨 ###
    # ��Ʊ��ÿ�ʽ���ʱ���������ǣ�����ʱӶ�����֮��������ʱӶ�����֮����ǧ��֮һӡ��˰, ÿ�ʽ���Ӷ����Ϳ�5��Ǯ
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    ## ���к�����reference_securityΪ����ʱ��Ĳο���ģ�����ı��ֻ���������֣���˴���'000300.XSHG'��'510300.XSHG'��һ���ģ�

    # ����ǰ����

    # ��ӡ���ݣ�������ʹ��
    # ��ӡ����ĳɽ���¼��������ʤ�����
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
    log.info('ԭʼ��������Ϊ��%s'%(str(g.sell_list)))
    log.info('ԭʼ��������Ϊ��%s'%(str(g.buy_list)))


    
# ��ӡ����ĳɽ���¼��������ʤ�����
def after_market_log_print(context):
    #�õ��������гɽ���¼
    log.info('$$�ֲ��ܼ�ֵ��%s' %(str(context.portfolio.positions_value)))
    i = 1
    for sec in context.portfolio.positions.keys():
        log.info('$$%d�� �ֲ֣�%s���ֲ�������%d��ӯ������� %f����ֵ��%f' %(i, \
                str(context.portfolio.positions[sec].security), \
                int((context.current_dt - context.portfolio.positions[sec].init_time).days), \
                float((context.portfolio.positions[sec].price - context.portfolio.positions[sec].avg_cost)/context.portfolio.positions[sec].avg_cost), \
                float(context.portfolio.positions[sec].value) 
                ))
        i += 1

    log.info('$$���ն�����Ϣ')
    orders = get_orders()
    for _order in orders.values():
        
        if _order.action == "open":
            log.info("�������룬��Ʊ%s���۸�%f"%(str(_order.security),float(_order.price)))

        if _order.action == "close":
            log.info("������������Ʊ%s���۸�%f"%(str(_order.security),float(_order.price)))

def rebalance(context):

    # buy_list,sell_list = get_list(context)
    buy_list,sell_list = get_list(context)
    
    print(buy_list,sell_list)
    # sell_list = set(sell_list)&set(context.portfolio.positions.keys())
    log.info('��������Ϊ��%s'%(str(sell_list)))
    
    for sec in sell_list:
        if sec in context.portfolio.positions.keys():
            order_target_value(sec, 0)

    
    # buy_list = set(buy_list)|set(context.portfolio.positions.keys())
    # print buy_list
    log.info('��������Ϊ��%s'%(str(buy_list)))
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
    # ��һ���ʱ��
    end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
        
    # ��ȡcsv�ļ�
    body = read_file("yield_pair_test.csv")
    pairs_info = pd.read_csv(BytesIO(body),index_col=0)
    
    # ѭ��������֤
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


    
        # ���ݱ��еĽṹ����
        beta = row['beta']
        alpha = row['alpha']
    
        gap = Q_yield - ( beta*P_yield + alpha ) 
    
     
        # buy_point_pd  = gap < mean - 0.5*std_deviation
        # sell_point_pd = gap > mean + 0.5*std_deviation
        mean = row['mean']
        std_deviation = row['std']
        
        if gap < (mean - 0.5*std_deviation):
            log.info("�����������������������б�%s"%(str(sec2)))

            buy_list.append(sec2)
        elif gap > (mean + 0.5*std_deviation):

            log.info("�����������������������б�%s"%(str(sec2)))
            sell_list.append(sec2)
        # ������
        log.info('�������ݿ�ʼ')
        log.info('����۸�P:%f'%float(P_now))
        log.info('ǰһ��۸�P:%f'%float(Price_pd_P[-1]))
        log.info('����۸�Q:%f'%float(Q_now))
        log.info('ǰһ��۸�Q:%f'%float(Price_pd_Q[-1]))
    
        log.info('P������Ϊ:%f'%float(P_yield))
        log.info('Q������Ϊ:%f'%float(Q_yield))
    
        
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
    
#     # ��ģ��һ֧��Ʊ
#     # '000300.XSHG'
#     # '601939.XSHG'
    
    
#     sec1 = '000300.XSHG'
#     sec2 = '601939.XSHG'
    
#     ncount = 30
#     # ��һ���ʱ��
#     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
    
#     Price_pd_P = get_price(sec1, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     Price_pd_Q = get_price(sec2, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']


#     current_data = get_current_data()
#     P_now = current_data[sec1].last_price
#     Q_now = current_data[sec2].last_price
    
#     P_yield = P_now/Price_pd_P[-1]
#     Q_yield = Q_now/Price_pd_Q[-1]


    
#     # �о�ģ����������
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

#     #     # 1. ��ȡ����
#     #     # ��ȡ��һ���ʱ��
#     #     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
#     #     # ��ȡ��������
#     #     ncount = 20
        
#     #     # # ��ȡ��������̼ۣ����жԱ���
#     #     Price_pd_P = get_price(row['p1'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     #     Price_pd_Q = get_price(row['p2'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
        
        
#     #     # ȷ��3��ʱ���ļ۸�
#     #     # ǰ��������̼�
#     #     P0 = Price_pd_P[-2]
#     #     P1 = Price_pd_P[-1]
        
        
#     #     Q0 = Price_pd_Q[-2]
#     #     Q1 = Price_pd_Q[-1]
        
#     #     # ��ǰ�ļ۸�
#     #     current_data = get_current_data()
#     #     P2 = current_data[row['p1']].last_price
#     #     Q2 = current_data[row['p2']].last_price
        
#     #     #2. �ж�Ŀǰ��״̬
#     #     # ���ж��Ƿ�������״̬�����������״̬����ֱ�Ӳ����ж�����״̬
#     #     # �ж������ǵ�ǰ�۸�ǰ1�������
#     #     res_diff_2 = (Q2 - Q1) - row['beta'] * (P2 - P1)
#     #     if res_diff_2 > 1.2* row['sigma']:
#     #     # if 0:
#     #         continue
#     #     else:
#     #         # ����Ƿ�����״̬�����ж��Ƿ�������״̬
#     #         # �ж�������ǰ1��۸�ǰ2��۸��ж��Ƿ�ƫ��
#     #         # �����ж�ƫ��״̬���Ƿ���Q�۸��ڻع�
#     #         res_diff_1 = (Q1 - Q0) - row['beta'] * (P1 - P0)
            
#     #         if res_diff_1 < -0.5* row['sigma'] :
                
#     #             # P�۸�仯����
#     #             Delta_P = (P2 - P1)/abs(P1 - P0)
#     #             # Q�۸�仯����
#     #             Delta_Q = (Q2 - Q1)/abs(Q1 - Q0)
                
#     #             # if (Delta_P >= 1) and (Delta_Q <= 1):
#     #             # if Delta_P > 1.5 * Delta_Q:
#     #             if 1:
#     #                 buy_list.append(row['p2'])
#     #             else:
#     #                 continue
#     #         else:
#     #             continue
#     #     # ��־��������Լ��ʹ��
#     #     log.info("P�۸�Ϊ%f,%f,%f"%(P0,P1,P2))
#     #     log.info("Q�۸�Ϊ%f,%f,%f"%(Q0,Q1,Q2))
#     #     log.info("res_diff_1Ϊ%f"%float((Q1 - Q0) - row['beta'] * (P1 - P0)))
#     #     log.info("res_diff_2Ϊ%f"%float((Q2 - Q1) - row['beta'] * (P2 - P1)))
#     #     log.info("sigmaΪ%f"%float(row['sigma']))
#     #     log.info("Delta_PΪ%f"%float((P2 - P1)/(P1 - P0)))
#     #     log.info("Delta_QΪ%f"%float((Q2 - Q1)/(Q1 - Q0)))



    
#     # # 3�����в���

#     # log.info("buy_listΪ%s"%buy_list)

    


# def sell(context):   
#     sell_list = get_sell_list(context)
#     sell_list = set(sell_list)&set(context.portfolio.positions.keys())
#     for sec in sell_list:
#         order_target_value(sec, 0)

# def get_sell_list(context):
    
#     sell_list = []

#     # ��ģ��һ֧��Ʊ
#     # '000300.XSHG'
#     # '601939.XSHG'
    
    
#     sec1 = '000300.XSHG'
#     sec2 = '601939.XSHG'
    
#     ncount = 30
#     # ��һ���ʱ��
#     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
    
#     Price_pd_P = get_price(sec1, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     Price_pd_Q = get_price(sec2, count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']


#     current_data = get_current_data()
#     P_now = current_data[sec1].last_price
#     Q_now = current_data[sec2].last_price
    
#     P_yield = P_now/Price_pd_P[-1]
#     Q_yield = Q_now/Price_pd_Q[-1]


    
#     # �о�ģ����������
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

#     #     # 1. ��ȡ����
#     #     # ��ȡ��һ���ʱ��
#     #     end_date = (context.current_dt  - timedelta(days=1)).strftime('%Y-%m-%d')
#     #     # ��ȡ��������
#     #     ncount = 20
        
#     #     # # ��ȡ��������̼ۣ����жԱ���
#     #     Price_pd_P = get_price(row['p1'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
#     #     Price_pd_Q = get_price(row['p2'], count =  ncount, end_date= end_date, frequency='1d', fields='close',fq = "pre")['close']
        
        
#     #     # ȷ��3��ʱ���ļ۸�
#     #     # ǰ��������̼�
#     #     P0 = Price_pd_P[-2]
#     #     P1 = Price_pd_P[-1]
        
        
#     #     Q0 = Price_pd_Q[-2]
#     #     Q1 = Price_pd_Q[-1]
        
#     #     # ��ǰ�ļ۸�
#     #     current_data = get_current_data()
#     #     P2 = current_data[row['p1']].last_price
#     #     Q2 = current_data[row['p2']].last_price
        
#     #     #2. �ж�Ŀǰ��״̬
#     #     # ���ж��Ƿ�������״̬�����������״̬����ֱ�Ӳ����ж�����״̬
#     #     # �ж������ǵ�ǰ�۸�ǰ1�������
#     #     res_diff_2 = (Q2 - Q1) - row['beta'] * (P2 - P1)
#     #     # if res_diff_2 > -0.2*row['sigma']:
#     #     if res_diff_2 >  0.5* row['sigma']:

#     #         sell_list.append(row['p2'])
            
#     # return sell_list
