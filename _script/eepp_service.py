"""
Windows service wrapper that runs EEPP Phase 1 and Phase 2 daily at fixed times.

- 05:00  -> Phase 1
- 13:05  -> Phase 2
"""
from __future__ import annotations

import os
import time
import schedule
import servicemanager
import win32event
import win32service
import win32serviceutil

PHASE_1_CMD = (
    'python "Shared drives\\R Drive\\Pricing\\_ERCOT Energy Price Predictor\\_script\\eepp_phase_1.py"'
)
PHASE_2_CMD = (
    'python "Shared drives\\R Drive\\Pricing\\_ERCOT Energy Price Predictor\\_script\\eepp_phase_2.py"'
)

class EEPPService(win32serviceutil.ServiceFramework):
    _svc_name_ = "EEPPService"
    _svc_display_name_ = "ERCOT Energy Price Predictor Service"

    def __init__(self, args):
        super().__init__(args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.running = True

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        self.running = False
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, ""),
        )
        self.main()

    @staticmethod
    def run_phase_1():
        os.system(PHASE_1_CMD)

    @staticmethod
    def run_phase_2():
        os.system(PHASE_2_CMD)

    def main(self):
        schedule.every().day.at("05:00").do(self.run_phase_1)
        schedule.every().day.at("13:05").do(self.run_phase_2)
        while self.running:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    win32serviceutil.HandleCommandLine(EEPPService)
