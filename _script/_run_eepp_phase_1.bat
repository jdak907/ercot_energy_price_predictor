@echo off
REM Run Phase 1 (downloads: solar, wind, system forecast, HROC, MORA; plots + Excel)
setlocal
python "Shared drives\R Drive\Pricing\_ERCOT Energy Price Predictor\_script\eepp_phase_1.py"
endlocal
