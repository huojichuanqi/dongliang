import asyncio
import ccxt
import logging
import time
import requests
from typing import Dict, Optional, Tuple

# ==================== 配置 ====================
API_KEY = ""
API_SECRET = ""
LEVERAGE = 0.0001
UPDATE_INTERVAL = 900
TOP_MOVERS_URL = "https://www.binance.com/fapi/v1/topMovers"
COOLDOWN_SEC = 10  # 重复追单冷却 秒

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()


class PureExchangeDataStrategy:
    def __init__(self, api_key: str, api_secret: str):
        self.exchange = ccxt.binance({
            "apiKey": api_key,
            "secret": api_secret,
            "options": {"defaultType": "future"},
        })
        self.exchange.load_markets()
        self._enable_hedge_mode()
        # ...其他初始化代码...
        self.blacklist = ['XNY']  # 新增黑名单列表
        self.allow_usdc = False  # 是否允许USDC交易对

    def _enable_hedge_mode(self):
        """确保双向持仓模式开启"""
        try:
            position_mode = self.exchange.fetch_position_mode(params={'subType': 'linear'})
            if not position_mode['hedged']:
                logger.info("正在启用双向持仓模式...")
                self.exchange.fapiPrivate_post_positionsidedual({'dualSidePosition': 'true'})
                logger.info("双向持仓模式已启用")
        except Exception as e:
            logger.error(f"设置持仓模式失败: {e}")
            raise

    def get_realtime_data(self) -> Tuple[Optional[str], Optional[str]]:
        """直接从交易所获取当前多空持仓"""
        try:
            positions = self.exchange.fetch_positions()
            long_pos = short_pos = None
            for p in positions:
                if float(p['contracts']) > 0:
                    if p['side'] == 'long':
                        long_pos = p['symbol']
                    else:
                        short_pos = p['symbol']
            return long_pos, short_pos
        except Exception as e:
            logger.error(f"获取持仓失败: {e}")
            return None, None

    def get_last_close_time(self, symbol: str, side: str) -> Optional[float]:
        """从交易所订单历史获取最近平仓时间"""
        try:
            orders = self.exchange.fetch_orders(
                symbol=symbol,
                since=int(time.time() * 1000) - 86400_000,  # 24小时内
                params={
                    'side': 'sell' if side == 'long' else 'buy',
                    'reduceOnly': 'true'
                }
            )
            if orders:
                latest_close = max(orders, key=lambda x: x['timestamp'])
                return latest_close['timestamp'] / 1000
        except Exception as e:
            logger.error(f"获取平仓记录失败: {e}")
        return None

    def should_skip(self, symbol: str, side: str) -> bool:
        """是否应该跳过交易（冷却期检查）"""
        last_close = self.get_last_close_time(symbol, side)
        if last_close:
            elapsed = time.time() - last_close
            logger.info(f"冷却检查 {symbol} {side}: 最后平仓 {elapsed:.1f}秒前")
            return elapsed < COOLDOWN_SEC
        return False

    def _filter_symbol(self, symbol: str) -> bool:
        """综合过滤条件"""
        # 检查黑名单
        if any(black.lower() in symbol.lower() for black in self.blacklist):
            logger.debug(f"过滤黑名单交易对: {symbol}")
            return False

        # 检查USDC交易对
        if not self.allow_usdc and ('USDC' in symbol or '/USDC:' in symbol):
            logger.debug(f"过滤USDC交易对: {symbol}")
            return False

        return True

    def _format_symbol(self, symbol: str) -> str:
        """正确格式化交易对符号（处理原始信号中的symbol）"""
        if not symbol:
            return symbol

        # 如果已经是完整格式则直接返回
        if symbol.endswith('/USDT:USDT'):
            return symbol

        # 处理原始信号（如"LUMIA" -> "LUMIA/USDT:USDT"）
        if 'USDT' not in symbol:
            return f"{symbol}/USDT:USDT"

        # 处理可能重复的情况（如"LUMIAUSDT" -> "LUMIA/USDT:USDT"）
        base = symbol.replace('USDT', '')
        return f"{base}/USDT:USDT"

    def close_position(self, symbol: str, side: str):
        """专用平仓方法"""
        try:
            positions = self.exchange.fetch_positions([symbol])
            for pos in positions:
                if float(pos['contracts']) > 0 and pos['side'] == side:
                    order_side = 'sell' if side == 'long' else 'buy'
                    self.exchange.create_order(
                        symbol=symbol,
                        type='market',
                        side=order_side,
                        amount=float(pos['contracts']),
                        params={
                            'positionSide': 'LONG' if side == 'long' else 'SHORT',
                            'newClientOrderId': 'x-TBzTen1X',
                            # 'reduceOnly': True  # 确保是平仓单
                        }
                    )
                    logger.info(f"已平仓 {symbol} {side} 仓位")
                    return True
            logger.warning(f"没有找到可平的 {symbol} {side} 仓位")
            return False
        except Exception as e:
            logger.error(f"平仓失败: {e}")
            return False

    def execute_trade(self, symbol: str, side: str):
        """执行交易（带格式校验）"""
        # 确保使用正确的symbol格式
        formatted_symbol = self._format_symbol(symbol)
        # print(formatted_symbol)
        if not formatted_symbol:
            logger.error(f"无效交易对: {symbol}")
            return

        if self.should_skip(formatted_symbol, side):
            logger.info(f"冷却跳过 {formatted_symbol} {side}")
            return
        # 获取余额和价格
        balance = float(self.exchange.fetch_balance()['USDT']['total'])
        price = self.exchange.fetch_ticker(formatted_symbol)['last']
        print(f"账户权益：{balance}")

        # 计算下单量
        amount = (balance * LEVERAGE) / price
        # print(amount)
        amount = float(self.exchange.amount_to_precision(formatted_symbol, amount))
        # print(amount)

        try:

            # 下单
            self.exchange.create_order(
                symbol=formatted_symbol,
                type="market",
                side="buy" if side == "long" else "sell",
                amount=amount,
                params={'newClientOrderId': 'x-TBzTen1X', "positionSide": "LONG" if side == "long" else "SHORT"}
            )
            logger.info(f"已开仓 {formatted_symbol} {side}")
        except Exception as e:
            logger.error(f"交易失败: {e}")

    async def run(self):
        """主策略循环"""
        while True:
            try:
                # 获取交易所实时数据
                current_long, current_short = self.get_realtime_data()
                logger.info(f"当前持仓 - 多头: {current_long}, 空头: {current_short}")

                # 获取动量信号并正确格式化
                try:
                    movers = requests.get(TOP_MOVERS_URL, timeout=5).json()
                    valid_movers = [
                        x for x in movers
                        if self._filter_symbol(x['symbol'])
                    ]
                    # print(valid_movers)
                    up_3 = next(
                        (self._format_symbol(x['symbol']) for x in
                         sorted(valid_movers, key=lambda x: x['createTimestamp'], reverse=True)
                         if x['eventType'] in ("PULLBACK")),
                        # if x['eventType'] in ("UP_1", "UP_2", "UP_3")),  # 修改为UP系列
                        None
                    )

                    down_3 = next(
                        (self._format_symbol(x['symbol']) for x in
                         sorted(valid_movers, key=lambda x: x['createTimestamp'], reverse=True)
                         if x['eventType'] in ("RALLY")),
                        # if x['eventType'] in ("DOWN_1", "DOWN_2", "DOWN_3")),  # 修改为DOWN系列
                        None
                    )
                    logger.info(f"动量筛选结果 - 多头: {up_3}, 空头: {down_3}")
                except Exception as e:
                    logger.error(f"获取信号失败: {e}")
                    await asyncio.sleep(UPDATE_INTERVAL)
                    continue

                # 多头调整
                if up_3:
                    if current_long and current_long != up_3:
                        self.close_position(current_long, "long")  # 正确平仓
                    if not current_long or current_long != up_3:
                        self.execute_trade(up_3, "long")  # 开新仓

                # 空头调整
                if down_3:
                    if current_short and current_short != down_3:
                        self.close_position(current_short, "short")  # 正确平仓
                    if not current_short or current_short != down_3:
                        self.execute_trade(down_3, "short")  # 开新仓

            except Exception as e:
                logger.error(f"策略出错: {e}")

            await asyncio.sleep(UPDATE_INTERVAL)


if __name__ == "__main__":
    strategy = PureExchangeDataStrategy(API_KEY, API_SECRET)
    asyncio.run(strategy.run())