"""
若干员工跟投策略的展示产品，计算最大回撤和超额最大回撤
"""

import pandas as pd
from datapy.pnl_stats.common.db_engine import con_db

con_market = con_db('bi_conf')
con_pnl = con_db('pnl_conf')
fund_list = ['锐天9号', '琴生1号', '锐天星耀1号', '麦克斯韦1号', '标准300指数增强1号', '中证1000指数增强1号']


def get_info():
    info_dict = {
        'Fund': fund_list,
        'BasePortfolio': ['live_rt9_stock', 'live_qinsheng_stock', 'live_xingyao1_stock', 'live_mksw1_stock',
                          'live_standard_hs300_stock', 'live_1000index1_stock'],
        'StartDate': ['2021-11-16', '2020-07-30', '2021-11-18', '2019-01-18', '2021-07-28', '2020-10-14'],
        'CompareIndex': ['000832-CSI-index', None, None, '000905-SH-index', '000300-SH-index', '000852-SH-index']
    }
    df = pd.DataFrame(info_dict)
    return df


def get_net(fund_list):
    sql = f"""
    SELECT Date,ProdName,Code,NAV,ACCNAV,AdjNAV
    FROM daily_return
    WHERE NAV IS NOT NULL
    AND ProdName IN {tuple(fund_list)}
    """
    net = pd.read_sql(sql, con_market, parse_dates=['Date'])
    net['Date'] = net['Date'].astype(str)
    return net


def get_index():
    sql = """
    SELECT Date,Uid,Close
    FROM basedata.daily_index_eodprice
    WHERE Date >= '2019-01-01'
    AND Uid != '000832-CSI-index'
    """
    idx1 = pd.read_sql(sql, con_market)
    sql = """
    SELECT TradeDate AS Date,IndexCode AS Uid,Close
    FROM pnl.ccbond_index
    """
    idx2 = pd.read_sql(sql, con_pnl)
    idx2['Uid'] += '-CSI-index'
    idx = pd.concat([idx1, idx2])
    idx = idx.pivot(index='Date', columns='Uid', values='Close')
    return idx


class NetCal:
    INFO = get_info()
    NET = get_net(fund_list=INFO['Fund'])
    INDEX = get_index()

    def __init__(self, fund_name, end_date):
        self.fund_name = fund_name
        self.end_date = end_date
        self.net = self.get_net()
        self.compare_index = self.INFO.set_index('Fund').loc[self.fund_name, 'CompareIndex']

    def get_net(self):
        start_date = self.INFO.set_index('Fund').loc[self.fund_name, 'StartDate']
        return self.NET[(self.NET['ProdName'] == self.fund_name) &
                        (self.NET['Date'] >= start_date) &
                        (self.NET['Date'] <= self.end_date)].set_index('Date').sort_index()

    def cal_max_withdraw(self):
        s = self.net['AdjNAV']
        df = pd.DataFrame(columns=['Net', 'Cummax', 'WithdrawRatio'])
        df['Net'] = s.sort_index().dropna(how='all')
        df['Cummax'] = df['Net'].cummax()
        df['WithdrawRatio'] = 1 - df['Net'] / df['Cummax']
        return df['WithdrawRatio'].max()

    def cal_excess_max_withdraw(self):
        if self.compare_index not in self.INDEX.columns:
            return
        df = self.net[['NAV', 'ACCNAV']].merge(self.INDEX[[self.compare_index]], left_index=True, right_index=True,
                                               how='left')
        df['CumRet'] = (df['ACCNAV'] - df['ACCNAV'].iloc[0]) / df['NAV'].iloc[0]
        df['IndexRet'] = df[self.compare_index] / df[self.compare_index].iloc[0] - 1
        df['CumExcessRet'] = df['CumRet'] - df['IndexRet']
        df['CummaxExcessRet'] = df['CumExcessRet'].cummax()
        df['ExcessRetWithdraw'] = df['CummaxExcessRet'] - df['CumExcessRet']
        return df['ExcessRetWithdraw'].max()


if __name__ == '__main__':
    end_date = '2023-03-03'
    res = {
        'Fund': [],
        'MaxWithDraw': [],
        'ExcessMaxWithDraw': [],
    }
    for fund_name in fund_list:
        self = NetCal(fund_name, end_date)
        res['Fund'].append(fund_name)
        res['MaxWithDraw'].append(self.cal_max_withdraw())
        res['ExcessMaxWithDraw'].append(self.cal_excess_max_withdraw())
    res_df = pd.DataFrame(res).set_index('Fund')

    res_df_pct = res_df.applymap(lambda x: '%.2f%%' % (x * 100))
    res_df_pct.columns = [x + '%' for x in res_df_pct.columns]

    res_all = pd.merge(res_df, res_df_pct, on='Fund')
    print(res_all)
