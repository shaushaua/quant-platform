import qlib
from qlib.constant import REG_CN
from qlib.tests.data import GetData
import os

# 设置数据目录
DATA_DIR = os.path.expanduser("~/.qlib/qlib_data/cn_data")

# 下载数据
if not os.path.exists(DATA_DIR):
    print("正在下载QLib数据...")
    GetData().qlib_data(target_dir=DATA_DIR, region=REG_CN)

# 初始化QLib
qlib.init(provider_uri=DATA_DIR, region=REG_CN)
print("QLib初始化成功！")
