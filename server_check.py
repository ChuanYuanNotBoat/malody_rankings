import requests
import time
import logging
from datetime import datetime

class ServerStatusMonitor:
    def __init__(self, url, check_interval=60, timeout=10):
        self.url = url
        self.check_interval = check_interval  # 默认1分钟检查一次
        self.timeout = timeout
        self.setup_logging()
    
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()  # 只输出到控制台，不写文件
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def check_server_status(self):
        """检查服务器状态并返回详细信息"""
        try:
            start_time = time.time()
            response = requests.get(self.url, timeout=self.timeout)
            response_time = round((time.time() - start_time) * 1000, 2)  # 毫秒
            
            status_info = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status_code': response.status_code,
                'response_time': response_time,
                'server_up': True,
                'headers': dict(response.headers),  # 包含响应头信息
                'content_length': len(response.content)  # 响应内容长度
            }
            
            # 详细输出状态信息
            print(f"\n=== 服务器状态检查 ===")
            print(f"时间: {status_info['timestamp']}")
            print(f"状态码: {status_info['status_code']}")
            print(f"响应时间: {status_info['response_time']}ms")
            print(f"内容长度: {status_info['content_length']} bytes")
            print(f"服务器状态: {'正常' if status_info['server_up'] else '异常'}")
            
            # 如果有重定向，显示重定向信息
            if response.history:
                print(f"重定向历史: {[r.status_code for r in response.history]}")
                print(f"最终URL: {response.url}")
            
            return status_info
            
        except requests.exceptions.Timeout:
            error_msg = "服务器请求超时"
            self.logger.error(error_msg)
            return self._create_error_response("Timeout", error_msg)
            
        except requests.exceptions.ConnectionError:
            error_msg = "服务器连接错误 - 可能服务器宕机或网络问题"
            self.logger.error(error_msg)
            return self._create_error_response("ConnectionError", error_msg)
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP错误: {str(e)}"
            self.logger.error(error_msg)
            return self._create_error_response("HTTPError", error_msg)
            
        except Exception as e:
            error_msg = f"检查服务器状态时发生错误: {str(e)}"
            self.logger.error(error_msg)
            return self._create_error_response("UnknownError", error_msg)
    
    def _create_error_response(self, error_type, message):
        """创建错误响应"""
        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status_code': error_type,
            'response_time': None,
            'server_up': False,
            'error_message': message
        }
    
    def start_monitoring(self):
        """开始监控服务器状态"""
        print(f"开始监控服务器: {self.url}")
        print(f"检查间隔: {self.check_interval}秒")
        print(f"超时设置: {self.timeout}秒")
        print("=" * 50)
        
        check_count = 0
        
        try:
            while True:
                status = self.check_server_status()
                check_count += 1
                print(f"检查次数: #{check_count}")
                print("=" * 50)
                
                # 如果服务器恢复正常，特别提醒
                if status['server_up'] and status['status_code'] == 200:
                    print("🎉 服务器已恢复正常！可以开始获取所需文件了。")
                    print("=" * 50)
                
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            print(f"\n监控已停止。总共进行了 {check_count} 次检查。")
        except Exception as e:
            print(f"监控过程中发生错误: {str(e)}")

def quick_status_check():
    """快速单次状态检查"""
    url = "https://m.mugzone.net/"
    
    print("执行快速状态检查...")
    try:
        response = requests.get(url, timeout=10)
        print(f"状态码: {response.status_code}")
        print(f"状态: {'正常' if response.status_code == 200 else '异常'}")
        print(f"响应头: {dict(response.headers)}")
        return response.status_code
    except Exception as e:
        print(f"错误: {e}")
        return None

if __name__ == "__main__":
    # 配置监控参数
    MALODY_URL = "https://m.mugzone.net/"
    
    # 询问用户想要哪种检查方式
    print("请选择检查方式:")
    print("1. 单次快速检查")
    print("2. 持续监控")
    
    choice = input("请输入选择 (1 或 2): ").strip()
    
    if choice == "1":
        quick_status_check()
    else:
        # 持续监控
        CHECK_INTERVAL = 60  # 1分钟检查一次
        TIMEOUT = 10  # 10秒超时
        
        monitor = ServerStatusMonitor(
            url=MALODY_URL,
            check_interval=CHECK_INTERVAL,
            timeout=TIMEOUT
        )
        
        monitor.start_monitoring()