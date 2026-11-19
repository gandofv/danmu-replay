"""Single-viewer WebSocket replay experiment. Run with Python 3.11+."""
import argparse
import asyncio
from dataclasses import dataclass, field

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


@dataclass
class Result:
    published: list[int] = field(default_factory=list)
    sessions: list[list[int]] = field(default_factory=list)
    displayed: list[int] = field(default_factory=list)
    timeline: list[str] = field(default_factory=list)

    def report(self):
        expected, actual = self.published, self.displayed
        lines = [*self.timeline, f"期望 {len(expected)} 条，实际显示 {len(actual)} 条"]
        for msg in dict.fromkeys(expected + actual):
            positions = [i for i, value in enumerate(actual, 1) if value == msg]
            if not positions:
                lines.append(f"缺少弹幕 {msg:02}")
            elif len(positions) > 1:
                lines.append(f"弹幕 {msg:02} 重复，位置 " + "、".join(map(str, positions)))
        lines.extend([f"发布顺序 {expected}", f"显示顺序 {actual}",
                      "PASS" if actual == expected else "FAIL"])
        return "\n".join(lines)


async def experiment(scenario, *, deduplicate=True):
    if scenario not in ("normal", "offline", "lost-ack"):
        raise ValueError(scenario)
    result = Result()
    ack = 0
    disconnected = asyncio.Event()
    seen = set()

    async def room(ws):
        nonlocal ack
        cursor = ack
        try:
            async for command in ws:
                if command.startswith("ACK "):
                    number = int(command.split()[1])
                    if number != cursor:
                        raise ValueError("unexpected acknowledgement")
                    ack = number
                    await ws.send("OK")
                elif command == "NEXT":
                    if cursor == len(result.published):
                        if cursor == 10:
                            await ws.send("END")
                            return
                        result.published.append(cursor + 1)
                    cursor += 1
                    await ws.send(str(result.published[cursor - 1]))
                else:
                    raise ValueError("unknown command")
        except ConnectionClosed:
            pass
        finally:
            disconnected.set()

    async def watch(uri, first):
        received = []
        result.sessions.append(received)
        async with connect(uri, proxy=None) as ws:
            result.timeline.append(f"连接 {len(result.sessions)}：从已确认编号 {ack:02} 恢复")
            while True:
                await ws.send("NEXT")
                frame = await ws.recv()
                if frame == "END":
                    return
                msg = int(frame)
                received.append(msg)
                if msg not in seen or not deduplicate:
                    result.displayed.append(msg)
                else:
                    result.timeline.append(f"客户端过滤重复投递 {msg:02}")
                seen.add(msg)
                cut = first and msg == 5 and scenario != "normal"
                if cut and scenario == "lost-ack":
                    result.timeline.append("05 已显示，发送 ACK 前断线")
                    ws.transport.abort()
                    return
                await ws.send(f"ACK {msg}")
                if await ws.recv() != "OK":
                    raise ValueError("missing acknowledgement receipt")
                if cut:
                    result.timeline.append("05 已确认，随后断线")
                    ws.transport.abort()
                    return

    async with asyncio.timeout(5), serve(room, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        uri = f"ws://127.0.0.1:{port}"
        await watch(uri, True)
        if scenario != "normal":
            await disconnected.wait()
            result.published.extend([6, 7])
            result.timeline.append("离线期间发布 06、07")
            await watch(uri, False)
            result.timeline.append(f"重连后收到 {result.sessions[-1]}")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["normal", "offline", "lost-ack"])
    parser.add_argument("--no-dedup", action="store_true", help="关闭客户端去重，演示缺陷")
    args = parser.parse_args()
    result = asyncio.run(experiment(args.scenario, deduplicate=not args.no_dedup))
    print(result.report())
    raise SystemExit(int(result.displayed != result.published))
