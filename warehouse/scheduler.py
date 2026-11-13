"""在线构建的后台调度：守护线程轮询驱动，写入与查询全程不暂停。

调度器只负责“什么时候推进一步”，所有状态与正确性仍在 OnlineBuilder 的
持久化状态机里；线程崩溃或进程退出后，候选状态完整落盘，重启用
OnlineBuilder.resume() 续建或 abandon() 废弃。单进程内通过 builder 实例
串行化，不会出现两个驱动者抢同一构建引用。
"""
import threading


class BuildScheduler:
    def __init__(self, builder, poll_seconds=0.05):
        self.builder = builder
        self.poll_seconds = poll_seconds
        self._thread = None
        self._stop = threading.Event()

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError('构建调度已在运行')
        self._stop.clear()
        self.builder.build_online()

        def loop():
            while not self._stop.is_set():
                state = self.builder.status()
                if state is None:
                    # 引用被显式废弃：调度结束，不自行重建
                    return
                phase = state['phase']
                if phase == 'active':
                    if self.builder.repo.head_sequence() <= state['watermark']:
                        self._stop.wait(self.poll_seconds)
                        continue
                    # 活动索引落后于表头：原子开启下一轮构建，rebuild 返回
                    # 的已是新候选状态，下一轮循环继续驱动
                    try:
                        self.builder.rebuild()
                    except Exception:
                        return
                    continue
                if phase == 'abandoned':
                    # 只可能来自 rebuild 的原子替换：新候选已挂在同一引用，
                    # 立即重新加载续建，绝不能空等旧对象
                    continue
                if phase == 'failed':
                    # 失败已落盘，查询仍可扫表；等待外部 resume/abandon
                    self._stop.wait(self.poll_seconds)
                    continue
                # scanning / catching_up / ready：统一交给状态机推进，
                # ready 落后时状态机会自行回到追平并重新证明
                try:
                    self.builder.resume()
                except Exception:
                    return

        self._thread = threading.Thread(target=loop, name='index-builder', daemon=True)
        self._thread.start()
        return self

    def stop(self, timeout=5):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def is_running(self):
        return self._thread is not None and self._thread.is_alive()
