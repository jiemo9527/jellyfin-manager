# Jellyfin Manager

Jellyfin 管理面板 + 自动化任务（用户生命周期/分流规则/备份/任务日志），支持 Web 管理与 Telegram 管理 Bot。

## 主要功能
- Web 管理面板：用户创建/禁用/续期/删除/改套餐
- 任务中心：同步用户、媒体库扫描、备份与日志
- 分流规则：规则矩阵与自动应用
- 设备清理：手动预览与定时清理
- Telegram 通知与管理 Bot


## 部署
```bash
cp .env.example .env
cp deploy/docker-compose.pull.yml docker-compose.yml
docker pull wanxve0000/jellyfin-manager:latest
docker compose up -d
```


### 访问
`http://<host>:18080/`





