@echo off
title SmartPillow PC Agent
cd /d "D:\agentcodex_sensors"
set WS_URL=ws://39.106.190.124:8000/ws/pc_agent
echo ============================================
echo   SmartPillow PC Agent
echo   Server: %WS_URL%
echo ============================================
echo.
echo Close this window to stop PC Agent.
echo.
py pc_agent.py
pause
