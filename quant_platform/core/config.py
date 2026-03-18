# -*- coding: utf-8 -*-
"""
核心配置 - 简化版，无外部依赖
"""

import os
from pathlib import Path
from typing import Optional, List, Dict, Any


class Config:
    """精简后的配置"""

    def __init__(self):
        # 通联配置
        self.tonglance_token: str = os.getenv("TONGLANCE_TOKEN", "")
        self.tonglance_sh_host: str = os.getenv("TONGLANCE_SH_HOST", "mdl-cloud-sh.datayes.com")
        self.tonglance_sh_port: int = int(os.getenv("TONGLANCE_SH_PORT", "19011"))
        self.tonglance_sz_host: str = os.getenv("TONGLANCE_SZ_HOST", "mdl-cloud-sz.datayes.com")
        self.tonglance_sz_port: int = int(os.getenv("TONGLANCE_SZ_PORT", "19011"))

        # 数据路径配置
        self.oss_data_path: str = os.getenv("OSS_DATA_PATH", "/2025")

        # 内存配置
        self.max_memory_gb: float = float(os.getenv("MAX_MEMORY_GB", "16.0"))

        # K线聚合配置
        self.kline_intervals: List[int] = [1, 5, 10, 15, 30, 60]

        # 回测配置
        self.default_account: float = 100_000_000.0
        self.benchmark: str = "000300.SH"
        self.open_cost: float = 0.0005
        self.close_cost: float = 0.0015

        # 日志配置
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        self.log_file: Optional[str] = os.getenv("LOG_FILE", None)

        # Docker 镜像配置
        self.docker_registry: str = os.getenv("DOCKER_REGISTRY", "registry.cn-shanghai.aliyuncs.com/quant-platform")
        self.base_image: str = os.getenv("BASE_IMAGE", "quant-platform-base:latest")

        # OSS 分布式回测配置
        self.oss_data_bucket: str = os.getenv("OSS_DATA_BUCKET", "quant-historical-data")
        self.oss_result_bucket: str = os.getenv("OSS_RESULT_BUCKET", "stock-mdl-data-result")

        # 阿里云 ECS 配置
        self.aliyun_access_key_id: str = os.getenv("ALIYUN_ACCESS_KEY_ID", "")
        self.aliyun_access_key_secret: str = os.getenv("ALIYUN_ACCESS_KEY_SECRET", "")
        self.aliyun_image_id: str = os.getenv("ALIYUN_IMAGE_ID", "")
        self.aliyun_security_group_id: str = os.getenv("ALIYUN_SECURITY_GROUP_ID", "")
        self.aliyun_vswitch_id: str = os.getenv("ALIYUN_VSWITCH_ID", "")

        # 回调配置
        self.callback_base_url: str = os.getenv("CALLBACK_BASE_URL", "")

    @classmethod
    def from_env(cls) -> "Config":
        """从环境变量加载配置"""
        return cls()

    def get_data_config(self) -> Dict[str, Any]:
        """获取数据相关配置"""
        return {
            "oss_data_path": self.oss_data_path,
            "max_memory_gb": self.max_memory_gb,
            "kline_intervals": self.kline_intervals,
            "tonglance": {
                "token": self.tonglance_token,
                "sh_host": self.tonglance_sh_host,
                "sh_port": self.tonglance_sh_port,
                "sz_host": self.tonglance_sz_host,
                "sz_port": self.tonglance_sz_port,
            }
        }

    def get_backtest_config(self) -> Dict[str, Any]:
        """获取回测相关配置"""
        return {
            "account": self.default_account,
            "benchmark": self.benchmark,
            "open_cost": self.open_cost,
            "close_cost": self.close_cost,
        }

    def get_distributed_config(self) -> Dict[str, Any]:
        """获取分布式回测相关配置"""
        return {
            "docker_registry": self.docker_registry,
            "base_image": self.base_image,
            "oss_data_bucket": self.oss_data_bucket,
            "oss_result_bucket": self.oss_result_bucket,
            "aliyun": {
                "access_key_id": self.aliyun_access_key_id,
                "access_key_secret": self.aliyun_access_key_secret,
                "image_id": self.aliyun_image_id,
                "security_group_id": self.aliyun_security_group_id,
                "vswitch_id": self.aliyun_vswitch_id,
            },
            "callback_base_url": self.callback_base_url,
        }

    def get_oss_path(self, date: str) -> Path:
        """
        获取OSS数据路径

        Args:
            date: 日期 (YYYY-MM-DD 或 YYYYMMDD)

        Returns:
            数据目录路径
        """
        date_str = date.replace("-", "")
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]

        return Path(self.oss_data_path) / f"{year}/{year}{month}/{year}{month}{day}"


# 全局配置实例
_config: Optional[Config] = None


def get_config() -> Config:
    """获取全局配置实例"""
    global _config
    if _config is None:
        _config = Config.from_env()
    return _config


def reload_config() -> Config:
    """重新加载配置"""
    global _config
    _config = Config.from_env()
    return _config
