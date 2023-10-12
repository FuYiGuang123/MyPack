import pyodbc
import json
import pandas as pd
import datetime

class Class_K线数据:
    def __init__(self):
        self.g_错误消息 = None
        self.g_连接状态 = False
        self.g_JS_K线数据 = {}
        self.表名称  = "K线历史数据"
        self.g_连接字符串 = (
            r"DRIVER={SQL Server Native Client 11.0};"
            r"SERVER=192.168.1.251;"
            r"DATABASE=行情数据;"
            r"UID=sa;"
            r"PWD=;"
        )

    def 初始化连接(self):
        try:
            self.conn = pyodbc.connect(self.g_连接字符串)
            self.g_连接状态 = True
            self.g_错误消息 = None
        except pyodbc.Error as e:
            self.g_连接状态 = False
            self.g_错误消息 = '数据库连接失败: ' + str(e)
        return self.g_连接状态

    def 查询_K线数据(self, 品种代码, 品种周期):
        查询数量 = 1
        返回数据 = None
        
        查询语句 = f"SELECT TOP {查询数量} 品种代码, 品种周期, JS_K线数据 FROM {self.表名称} WHERE 品种代码 = ? AND 品种周期 = ?"
        try:
            cursor = self.conn.cursor()
            cursor.execute(查询语句, 品种代码, 品种周期)
            row = cursor.fetchone()
            if not row:
                self.g_错误消息 = "没有找到匹配的记录"
                返回数据 = False
            else:
                解析后数据 = row.JS_K线数据
                self.g_错误消息 = None
                返回数据 = 解析后数据
        except pyodbc.Error as e:
            self.g_错误消息 = '查询K线数据失败: ' + str(e)
            返回数据 = False
        return 返回数据

    def 查询_最新K线(self, 市场名称, 品种代码, 品种周期):
        查询数量 = 1
        返回数据 = None

        查询语句 = f"SELECT TOP {查询数量} * FROM [K线历史数据].[dbo].[市场数据] WHERE 市场名称 = ?"
        try:
            cursor = self.conn.cursor()
            cursor.execute(查询语句, 市场名称)
            row = cursor.fetchone()
            if not row:
                self.g_错误消息 = "没有找到匹配的记录"
                返回数据 = False
            else:
                解析后数据 = json.loads(row.最新K线列表)
                for item in 解析后数据['最新K线列表']:
                    if item['品种代码'] == 品种代码 and item['品种周期'] == 品种周期:
                        返回数据 = item
                        self.g_错误消息 = None
                        break
                if 返回数据 is None:
                    self.g_错误消息 = '查询_最新K线失败: 数据库找不到这条记录'
        except pyodbc.Error as e:
            self.g_错误消息 = '查询_最新K线失败: ' + str(e)
            返回数据 = False
        return 返回数据

    def K线数据刷新(self, K线数据, 最新K线数据):
        lastOpenTime = K线数据["K线数据"]["开盘时间"][-1]

        if 最新K线数据["开盘时间"] == lastOpenTime:
            K线数据["K线数据"]["开盘时间"][-1] = 最新K线数据["开盘时间"]
            K线数据["K线数据"]["最低价格"][-1] = 最新K线数据["最低价格"]
            K线数据["K线数据"]["成交数量"][-1] = 最新K线数据["成交数量"]
            K线数据["K线数据"]["成交金额"][-1] = 最新K线数据["成交金额"]
            K线数据["K线数据"]["开盘价格"][-1] = 最新K线数据["开盘价格"]
            K线数据["K线数据"]["最高价格"][-1] = 最新K线数据["最高价格"]
            K线数据["K线数据"]["完结状态"][-1] = 最新K线数据["完结状态"]
            K线数据["K线数据"]["收盘价格"][-1] = 最新K线数据["收盘价格"]
            K线数据["K线数据"]["品种代码"][-1] = 最新K线数据["品种代码"]
            K线数据["K线数据"]["品种名称"][-1] = 最新K线数据["品种名称"]
            K线数据["K线数据"]["品种周期"][-1] = 最新K线数据["品种周期"]
        else:
            K线数据["K线数据"]["开盘时间"].append(最新K线数据["开盘时间"])
            K线数据["K线数据"]["最低价格"].append(最新K线数据["最低价格"])
            K线数据["K线数据"]["成交数量"].append(最新K线数据["成交数量"])
            K线数据["K线数据"]["成交金额"].append(最新K线数据["成交金额"])
            K线数据["K线数据"]["开盘价格"].append(最新K线数据["开盘价格"])
            K线数据["K线数据"]["最高价格"].append(最新K线数据["最高价格"])
            K线数据["K线数据"]["完结状态"].append(最新K线数据["完结状态"])
            K线数据["K线数据"]["收盘价格"].append(最新K线数据["收盘价格"])
            K线数据["K线数据"]["品种代码"].append(最新K线数据["品种代码"])
            K线数据["K线数据"]["品种名称"].append(最新K线数据["品种名称"])
            K线数据["K线数据"]["品种周期"].append(最新K线数据["品种周期"])
        return K线数据



def get_K线数据 (品种名称,品种周期):
    K线数据 = Class_K线数据()
    if K线数据.初始化连接():
        print("数据库连接成功！")
        
    else:
        print(f"数据库连接失败: {K线数据.g_错误消息}")


    查询结果 = K线数据.查询_K线数据(品种名称, 品种周期)
    if 查询结果:
        #  print(f"查询到K线数据: {查询结果}")
        return 查询结果
    else:
        # print(f"查询K线数据失败: {G_k线数据实例.g_错误消息}")
        return K线数据.g_错误消息


def K线数据_to_pd(K线数据_Str):
    # 从数据库K线模块获取K线数据，假设返回的数据是JSON格式的字符串
    json_data = json.loads(K线数据_Str)  # 调用 将其解析为JSON对象

    # 从JSON数据中提取K线数据
    data = json_data["K线数据"]  # 从JSON对象中获取名为 "K线数据" 的数据

    # 将时间戳转换为日期时间对象
    # dates = pd.to_datetime(data["开盘时间"], unit="ms" ) + pd.Timedelta(hours=8) # 使用pd.to_datetime将时间戳转换为日期时间对象，单位为毫秒
    dates = pd.to_datetime(data["开盘时间"], unit='ms' ,origin='1970-01-01 08:00:00') # 使用pd.to_datetime将时间戳转换为日期时间对象，单位为毫秒
    # dates = pd.to_datetime(data["开盘时间"], unit='ms' ,utc=True) # 使用pd.to_datetime将时间戳转换为日期时间对象，单位为毫秒

    
    # 创建一个DataFrame来存储K线数据
    df = pd.DataFrame({
    "datetime": dates,         # 将日期时间对象作为 "datetime" 列
    "open": data["开盘价格"],    # 开盘价格列
    "high": data["最高价格"],    # 最高价格列
    "low": data["最低价格"],     # 最低价格列
    "close": data["收盘价格"],   # 收盘价格列
    "volume": data["成交数量"],  # 成交数量列
    })


    # df["datetime"] = df["datetime"].dt.tz_convert('Asia/Shanghai')
    # 将日期时间列设置为DataFrame的索引
    df.set_index("datetime", inplace=True)  # 将 "datetime" 列设置为索引，并在原地修改DataFrame
    # df.set_index("datetime")  # 将 "datetime" 列设置为索引，并在原地修改DataFrame
    return df


# def stamp2time(timestamp): #时间歌日期医
#     """
#     功能:将时间戳转换成日期函数 例如: 166708276268 ==》2020-11-30 11:51:16参数: timestamp 时间戳，类型 double 例如: 160678276268
#     返回值:日期，类型:字符串 2028-11-38 11:51:16
#     a a 
#     """
#     time_local = time.localtime(timestamp/1000)
#     dt = time.strftime("%Y-%m-%d %H:%M:%s"time local)
#     return dt
def K线数据周期合并(data, 周期):
    """
    将数据按指定的时间周期进行重采样。

    参数:
    - data: 输入的DataFrame数据
    - 周期: 重采样的时间周期，可以是以下之一:
        * 分钟: 'T' 或 'min' (每分钟), '5T' (每5分钟), '15T' (每15分钟), ...
        * 小时: 'H' (每小时), '2H' (每2小时), '4H' (每4小时), ...
        * 天: 'D' (每天)
        * 周: 'W' 或 'W-SUN' (以周日为每周的开始), 'W-MON' (以周一为每周的开始), ...
        * 月: 'M' (每月的最后一个日历日), 'MS' (每月的第一个日历日)
        * 季度: 'Q' (每季度的最后一个月的最后一个日历日), 'QS' (每季度的第一个月的第一个日历日), 'Q-JAN', 'Q-FEB', ...
        * 年: 'A' 或 'Y' (每年的最后一个日历日), 'AS' 或 'YS' (每年的第一个日历日), 'A-JAN', 'A-FEB', ...

    返回:
    - 重采样后的DataFrame数据
    """
    # data_1d = data.resample('1D').agg(lambda x: x[-1] if len(x) > 0 else None)
    resampled_data = data.resample(周期,closed='left').agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    })
    if 周期 == 'W-MON':
        resampled_data.index = resampled_data.index + pd.DateOffset(days=-7)#时间偏移
    return resampled_data
    
    resampled_data = data.resample(周期, closed='right').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})


if __name__ == '__main__':
    # print(get_K线数据("ETC-USDT-SWAP", "5Min"))
    
    # json_data = json.loads(get_K线数据("ETC-USDT-SWAP", "5Min"))  # 调用 将其解析为JSON对象
    # # json_data["data"]
    # 索引 = -2
    # print(datetime.datetime.fromtimestamp(json_data["K线数据"]["开盘时间"][索引]/1000),
    # json_data["K线数据"]["开盘时间"][索引],
    # "开盘价格",json_data["K线数据"]["开盘价格"][索引],
    # "收盘价格",json_data["K线数据"]["收盘价格"][索引]
    # )
    data = (K线数据_to_pd(get_K线数据("BTC-USDT-SWAP", "5Min")))
    data = (K线数据周期合并(data, 'W-MON'))  # 输出处理后的DataFrame
    print(data)







# G_k线数据实例 = Class_K线数据()
# if G_k线数据实例.初始化连接():
#     print("数据库连接成功！")
# else:
#     print(f"数据库连接失败: {G_k线数据实例.g_错误消息}")




# 查询结果 = G_k线数据实例.查询_K线数据("BTC-USDT-SWAP", "1D")
# if 查询结果:
#     print(f"查询到K线数据: {查询结果}")
# else:
#     print(f"查询K线数据失败: {G_k线数据实例.g_错误消息}")

