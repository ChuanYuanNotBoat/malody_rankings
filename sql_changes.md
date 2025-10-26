# SQL数据库结构变更记录

## 版本 2.0 - 添加UID支持

### 变更内容

1. 在 `player_identity` 表中添加 `uid` 字段
2. 在 `player_aliases` 表中添加 `uid` 字段
3. 在 `player_rankings` 表中添加 `uid` 字段
4. 为各表的 `uid` 字段创建索引

### SQL语句

```sql
-- 添加uid字段
ALTER TABLE player_identity ADD COLUMN uid TEXT;
ALTER TABLE player_aliases ADD COLUMN uid TEXT;
ALTER TABLE player_rankings ADD COLUMN uid TEXT;

-- 创建索引
CREATE INDEX idx_player_identity_uid ON player_identity(uid);
CREATE INDEX idx_player_aliases_uid ON player_aliases(uid);
CREATE INDEX idx_player_rankings_uid ON player_rankings(uid);
```
