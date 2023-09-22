#!/usr/bin/python
"""Prometheus exporter for an Arris Surfboard SB6141."""

import logging
import re
import time

import click
import lxml.etree
import prometheus_client  # type: ignore
import requests

logger = logging.getLogger()
DURATION_RE = re.compile(
    r"((?P<days>\d+) days )?(?P<hours>\d+)h:(?P<minutes>\d{1,2})m:(?P<seconds>\d{1,2})s"
)
FLOAT_RE = re.compile(r"\d+(\.\d+)?")
SECONDS = {"days": 24 * 60 * 60, "hours": 60 * 60, "minutes": 60, "seconds": 1}
STATUS_PAGE = "http://192.168.100.1/indexData.htm"
SIGNAL_PAGE = "http://192.168.100.1/cmSignalData.htm"
SIGNAL_DATA = {
    "Downstream": [
        "Frequency",
        "Signal to Noise Ratio",
        "Downstream Modulation",
        "Power Level",
    ],
    "Upstream": ["Frequency", "Ranging Service ID", "Symbol Rate", "Power Level"],
    "Signal Status (Codewords)": [
        "Total Unerrored Codewords",
        "Total Correctable Codewords",
        "Total Uncorrectable Codewords",
    ],
}
TASKS = [
    "DOCSIS Downstream Channel Acquisition",
    "DOCSIS Ranging",
    "Establish IP Connectivity using DHCP",
    "Establish Time Of Day",
    "Transfer Operational Parameters through TFTP",
    "Register Connection",
    "Cable Modem Status",
    "Initialize Baseline Privacy",
]
UPTIME = "System Up Time"
TIME = "Current Time and Date"
TASK_STATES = ["Not started", "Offline", "Done", "Operational", "Other"]
METRIC_PREFIX = "surfboard"


def to_metric(words: list[str]) -> str:
    """Return a metric-style name for a list of words."""
    words = [
        word.replace("(", "").replace(")", "").replace(" ", "_").strip()
        for word in words
    ]
    return "_".join([METRIC_PREFIX] + words).lower()


class SurfboardExporter:
    """Exporter."""

    def __init__(self, refresh_rate: int):
        """Initialize Prometheus metrics."""
        self.tasks = {
            task: prometheus_client.Enum(to_metric([task]), task, states=TASK_STATES)
            for task in TASKS
        }
        self.signals = {
            (table, key): prometheus_client.Gauge(
                to_metric([table, key]), f"{table}: {key}", ["channel"]
            )
            for table, keys in SIGNAL_DATA.items()
            for key in keys
        }
        self.uptime = prometheus_client.Gauge(to_metric([UPTIME]), UPTIME)
        self.refresh_rate = refresh_rate
        self.session = requests.Session()

    def update_status(self):
        """Update status page metrics."""
        resp = self.session.get(STATUS_PAGE)
        if not resp.ok:
            return
        for row in lxml.etree.HTML(resp.text).xpath("//tr"):
            values = [r.text for r in row.getchildren()]
            if len(values) != 2 or not all(v for v in values):
                continue
            task, value = (v.strip() for v in values)
            if task in TASKS:
                if value in TASK_STATES:
                    self.tasks[task].state(value)
                else:
                    self.tasks[task].state("Other")
                    logger.warning("Unknown state %s for task %s.", value, task)
            elif task == TIME:
                pass
            elif task == UPTIME:
                parts = {
                    k: int(v or 0)
                    for k, v in DURATION_RE.match(value).groupdict().items()
                }
                uptime = sum(num * SECONDS[unit] for unit, num in parts.items())
                self.uptime.set(uptime)
            else:
                logger.warning("Unknown row %s", task)

    def update_signal(self):
        """Update signal page metrics."""
        resp = self.session.get(SIGNAL_PAGE)
        if not resp.ok:
            return
        for table in lxml.etree.HTML(resp.text).xpath("//table"):
            table_rows = table.findall("tbody/tr")
            if len(table_rows) < 3:
                continue
            header, channels, *rows = table_rows
            table_name = [i.text.strip() for i in header.iterdescendants() if i.text][0]
            channel_ids = [int(i.text) for i in channels.findall("td")[1:]]
            for row in rows:
                key, *values = [i.text for i in row.findall("td")]
                key = next(
                    (k for k in SIGNAL_DATA[table_name] if key.startswith(k)), None
                )
                if not key:
                    continue
                for channel, value in zip(channel_ids, values, strict=True):
                    if match := FLOAT_RE.match(value):
                        self.signals[table_name, key].labels(channel).set(
                            float(match.group())
                        )

    def main_loop(self):
        """Loop to update metrics then sleep."""
        while True:
            start = time.time()
            self.update_status()
            self.update_signal()
            time.sleep(max(start + self.refresh_rate - time.time(), 5))


@click.command
@click.option("--refresh_rate", type=int, default=30)
@click.option("--port", type=int, default=8786)
def main(refresh_rate: int, port: int) -> None:
    """Export SB6141 metrics."""
    prometheus_client.start_http_server(port)
    SurfboardExporter(refresh_rate).main_loop()


if __name__ == "__main__":
    main()
