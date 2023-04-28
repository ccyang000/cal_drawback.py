"""
若干员工跟投策略的展示产品，计算最大回撤和超额最大回撤
"""
import click
import pandas as pd
from loguru import logger

fund_list = ['锐天9号', '琴生1号', '锐天星耀1号', '麦克斯韦1号', '标准300指数增强1号', '中证1000指数增强1号', '锐天103号']


def get_info():
    info_dict = {
        'Fund': fund_list,
        'BasePortfolio': ['live_rt9_stock', 'live_qinsheng_stock', 'live_xingyao1_stock', 'live_mksw1_stock',
                          'live_standard_hs300_stock', 'live_1000index1_stock', 'live_rt103_stock'],
        'StartDate': ['2021-11-16', '2020-07-30', '2021-11-18', '2019-01-18', '2021-07-28', '2020-10-14', '2023-03-15'],
        'CompareIndex': ['000832-CSI-index', None, None, '000905-SH-index', '000300-SH-index', '000852-SH-index',
                         '000905-SH-index']
    }
    df = pd.DataFrame(info_dict)
    return df


def get_net(fund_list):
    net = pd.read_csv(r'C:\Users\admin\Desktop\net.csv')
    net = net[net['ProdName'].isin(fund_list)]
    net['Date'] = net['Date'].astype(str)
    return net


def get_index():
    idx = pd.read_csv(r'C:\Users\admin\Desktop\idx_eodprice.csv')
    idx = idx.pivot(index='Date', columns='Uid', values='Close')
    return idx


class NetCal:
    INFO = get_info()
    NET = get_net(fund_list=INFO['Fund'])
    INDEX = get_index()

    def __init__(self, fund_name, end_date):
        assert end_date is not None
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


# 命令行执行：python 脚本.py --end-date 'yyyy-mm-dd'
@click.command()
@click.option('--end-date', help='统计截止日期，%y-%m-%d')
def run(end_date):
    logger.info('start')
    res = {
        'Fund': [],
        'MaxWithDraw': [],
        'ExcessMaxWithDraw': [],
    }
    for fund_name in fund_list:
        logger.info(fund_name)
        self = NetCal(fund_name, end_date)
        res['Fund'].append(fund_name)
        res['MaxWithDraw'].append(self.cal_max_withdraw())
        res['ExcessMaxWithDraw'].append(self.cal_excess_max_withdraw())
    res_df = pd.DataFrame(res).set_index('Fund')

    res_df_pct = res_df.applymap(lambda x: '%.2f%%' % (x * 100))
    res_df_pct.columns = [x + '%' for x in res_df_pct.columns]

    res_all = pd.merge(res_df, res_df_pct, on='Fund')
    logger.info('\n' + str(res_all))


if __name__ == '__main__':
    run()
